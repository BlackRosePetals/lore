# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Smoke tests for a user directory advertised apart from the auth service.

`[environment.endpoint] user_url` defines where clients resolve user
IDs to display names over `lore.user.v1.UserService`. Without it the
client speaks the `UrcAuthApi` directory RPCs to `auth_url`, covered by
`test_auth_online.py`.
"""

import logging
from types import SimpleNamespace

import pytest
from error_types import LoreException
from lore_server import (
    _kill_server_by_pid,
    allocate_free_port,
    generate_server_config,
    launch_lore_server,
)
from mock_auth_server import (
    USER1,
    USER2,
    MockAuthServer,
    MockUser,
    user_list_response,
)
from test_auth_online import authz_resources, commit_file, provision_owner

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.xdist_group("user_service")


CONFIG = """

# --- appended by test_user_service.py: auth service and a separate directory ---
[environment.endpoint]
auth_url = "{auth_url}"
user_url = "{user_url}"

[server.auth]
jwt_issuer = "{issuer}"
jwt_audience = ["{audience}"]

[server.auth.jwk]
endpoint = "{jwks_url}"
"""


@pytest.fixture(scope="module")
def stubs():
    """The auth service and the directory, as two independent stubs."""
    auth = MockAuthServer().start()
    directory = MockAuthServer(issuer="urc-mock-directory").start()
    try:
        yield SimpleNamespace(auth=auth, directory=directory)
    finally:
        directory.stop()
        auth.stop()


@pytest.fixture(autouse=True)
def _reset_stubs(stubs):
    stubs.auth.reset()
    stubs.directory.reset()


def _launch(request, tmp_path_factory, executable, stubs, user_url):
    """A lore server authenticating against the auth stub and advertising
    `user_url` as its user directory. Yields the shape `provision_owner`
    reads: the auth stub as `mock`, plus the remote URL."""
    shared_port = allocate_free_port()
    ports = {
        "quic": shared_port,
        "grpc": shared_port,
        "http": allocate_free_port(),
        "internal": allocate_free_port(),
    }
    server_root, server_env = generate_server_config(request, tmp_path_factory, ports)
    config_path = server_root / "lore-server" / "config" / "gha.toml"
    config_path.write_text(
        config_path.read_text()
        + CONFIG.format(
            auth_url=stubs.auth.auth_url,
            user_url=user_url,
            issuer=stubs.auth.issuer,
            audience=stubs.auth.audience[0],
            jwks_url=stubs.auth.jwks_url,
        )
    )
    server_proc, server_log_path, server_log_fd = launch_lore_server(
        server_root, server_env, executable
    )
    try:
        yield SimpleNamespace(
            mock=stubs.auth,
            directory=stubs.directory,
            user_url=user_url,
            remote_url=f"lore://127.0.0.1:{shared_port}/",
            server_log=server_log_path,
        )
    finally:
        _kill_server_by_pid(
            server_proc.pid, server_log_path, label="user-service server"
        )
        server_log_fd.close()


@pytest.fixture(scope="module")
def service_env(request, tmp_path_factory, lore_server_executable_path, stubs):
    """The directory reached at `127.0.0.1`, a host the tokens' `aud` names."""
    yield from _launch(
        request,
        tmp_path_factory,
        lore_server_executable_path,
        stubs,
        stubs.directory.auth_url,
    )


@pytest.fixture(scope="module")
def foreign_service_env(request, tmp_path_factory, lore_server_executable_path, stubs):
    """The same directory reached as `localhost`, which no token names."""
    yield from _launch(
        request,
        tmp_path_factory,
        lore_server_executable_path,
        stubs,
        f"http://localhost:{stubs.directory.grpc_port}",
    )


@pytest.fixture(scope="function")
def make_actor(new_lore_repo, scratch_dir):
    """`(env, label) -> actor`: an isolated token store plus a repo factory
    targeting `env`'s server. Curried per environment, so the two servers in
    this module can share `provision_owner`."""

    def _for(env):
        def _make_actor(label: str):
            auth_store = scratch_dir(f"auth-store-{label}", create=True)
            environment_vars = {"LORE_AUTH_PATH": str(auth_store)}

            def make_repo(**kwargs):
                kwargs.setdefault("remote_url", env.remote_url)
                kwargs.setdefault("create_repo", False)
                kwargs.setdefault("environment_vars", dict(environment_vars))
                return new_lore_repo(**kwargs)

            return SimpleNamespace(make_repo=make_repo, auth_store=auth_store)

        return _make_actor

    return _for


@pytest.mark.smoke
def test_names_are_asked_of_the_advertised_directory(
    service_env, make_actor, lore_library_path
):
    """Another user's name is fetched from `user_url` with `UserGet`, the
    repository id bytes as the partition."""
    owner = provision_owner(service_env, make_actor(service_env), "owner", USER1)
    directory = service_env.directory
    directory.on("UserGet", bearer=owner.authz_token).respond(user_list_response(USER2))

    result = owner.repo.auth_user_info_capi(lore_library_path, USER2.user_id)

    assert result == 0, f"resolving through the directory failed with FFI code {result}"
    [request] = directory.requests_for("UserGet")
    assert list(request["id"]) == [USER2.user_id]
    assert request["partition"].hex() == owner.resource_id.removeprefix("urc-")


@pytest.mark.smoke
def test_a_directory_outside_the_tokens_domains_is_sent_nothing(
    foreign_service_env, make_actor, lore_library_path
):
    """The exchanged token's `aud` does not cover `localhost`, so the client
    refuses to present it there, exactly as it would refuse a Lore server at
    that host."""
    env = foreign_service_env
    owner = provision_owner(env, make_actor(env), "owner", USER1)
    env.directory.on("UserGet").respond(user_list_response(USER2))

    result = owner.repo.auth_user_info_capi(lore_library_path, USER2.user_id)

    assert result != 0, "a token must not reach a host its claims do not name"
    assert env.directory.calls["UserGet"] == 0


def query_locks_by_owner(owner, name: str, supplied_token: str) -> str:
    """`lore lock query --owner <name>` with a caller-supplied access token,
    which skips the exchange. The login token rides along as the identity
    token, since a supplied credential is never looked up in the store."""
    return owner.repo.run(
        urc_args=["lock", "query", "--owner", name],
        identity_token=owner.login_token,
        access_token=supplied_token,
    )


@pytest.mark.smoke
def test_a_supplied_access_token_is_sent_to_a_directory_its_claims_name(
    service_env, make_actor
):
    """A caller-supplied `--access-token` bypasses the exchange and with it
    the exchange's recipient check, so the user service helper checks the token
    against the directory host itself. Inside the token's `aud` the token is
    sent, with no exchange made."""
    env = service_env
    owner = provision_owner(env, make_actor(env), "owner", USER1)
    supplied = env.mock.mint_token(USER1, resources=authz_resources(owner.resource_id))
    env.directory.on("UserFind", bearer=supplied).respond(user_list_response(USER2))
    exchanges_before = env.mock.calls["ExchangeUserTokenForMultiresourceToken"]

    query_locks_by_owner(owner, USER2.preferred_username, supplied)

    assert env.directory.calls["UserFind"] == 1
    assert (
        env.mock.calls["ExchangeUserTokenForMultiresourceToken"] == exchanges_before
    ), "a supplied access token is used as is, never exchanged"


@pytest.mark.smoke
def test_a_supplied_access_token_is_checked_against_the_directory_host(
    foreign_service_env, make_actor
):
    """The same supplied token against a directory advertised as `localhost`,
    a host its `aud` does not name: refused before any request. A rogue server
    cannot redirect a caller's own token to a host of its choosing by
    advertising it as the directory."""
    env = foreign_service_env
    owner = provision_owner(env, make_actor(env), "owner", USER1)
    supplied = env.mock.mint_token(USER1, resources=authz_resources(owner.resource_id))
    env.directory.on("UserFind").respond(user_list_response(USER2))

    with pytest.raises(LoreException):
        query_locks_by_owner(owner, USER2.preferred_username, supplied)

    assert env.directory.calls["UserFind"] == 0


def lock_query_by_owner_name(env, make_actor, name: str) -> str:
    """USER1 locks a file, then queries the locks owned by `name`. The lock is
    listed only when the directory resolves `name` to USER1. The raw output is
    checked rather than parsed: on macOS the in-process gRPC stub's library
    logs to the child's stderr after fork, and those lines land in the output."""
    owner = provision_owner(env, make_actor(env), "owner", USER1)
    commit_file(owner.repo, "locked.txt", "content")
    owner.repo.lock_acquire("locked.txt")
    return owner.repo.run(["lock", "query", "--owner", name])


@pytest.mark.smoke
def test_a_lock_owner_is_found_by_username(service_env, make_actor):
    service_env.directory.on("UserFind", username=USER1.preferred_username).respond(
        user_list_response(USER1)
    )

    output = lock_query_by_owner_name(service_env, make_actor, USER1.preferred_username)

    assert "locked.txt" in output


@pytest.mark.smoke
def test_a_lock_owner_is_found_by_display_name(service_env, make_actor):
    directory = service_env.directory
    directory.on("UserFind", username=USER1.display_name).respond(user_list_response())
    directory.on("UserFind", display_name=USER1.display_name).respond(
        user_list_response(USER1)
    )

    output = lock_query_by_owner_name(service_env, make_actor, USER1.display_name)

    assert "locked.txt" in output


@pytest.mark.smoke
def test_a_shared_display_name_finds_no_lock_owner(service_env, make_actor):
    """Two users share USER1's display name: the name resolves to no one,
    rather than to whichever user the directory listed first."""
    namesake = MockUser("mock-user-3", USER1.display_name, "namesake")
    directory = service_env.directory
    directory.on("UserFind", username=USER1.display_name).respond(user_list_response())
    directory.on("UserFind", display_name=USER1.display_name).respond(
        user_list_response(USER1, namesake)
    )

    output = lock_query_by_owner_name(service_env, make_actor, USER1.display_name)

    assert "locked.txt" not in output
