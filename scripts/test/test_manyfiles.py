# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
import logging
import os
import random
import shutil
import subprocess
import time

import pytest

from lore import Lore
from store_layout import assert_all_index_entries_reachable

logger = logging.getLogger(__name__)

# The small files are laid out as TOP_DIRS x SUB_DIRS directories of FILES_PER_DIR
# files each. Ten top directories made 10 000 files, and writing, cloning,
# comparing and removing them made this the longest test in the suite; three
# still keep the mid-commit kill below landing inside the commit, which the
# second commit checks by having something left to commit.
TOP_DIRS = 3
SUB_DIRS = 10
FILES_PER_DIR = 100


@pytest.mark.smoke
@pytest.mark.runs_in_process("terminates the process carrying out a commit")
def test_file(new_lore_repo, lore_executable_path):
    repo: Lore = new_lore_repo()
    for i in range(TOP_DIRS):
        subpath = str(i)
        repo.make_dirs(subpath)
        for j in range(SUB_DIRS):
            subsubpath = os.path.join(subpath, str(j))
            repo.make_dirs(os.path.join(subsubpath))
            for k in range(FILES_PER_DIR):
                with repo.open_file(
                    os.path.join(subsubpath, str(k) + ".uasset"), "w+b"
                ) as output_file:
                    # Minimum 5 KiB per file so every write crosses the compression
                    # threshold, which keeps the commit busy long enough that the
                    # mid-commit kill below lands deterministically.
                    output_file.write(os.urandom(5120 + k + (10 * i * k * j)))

    # One file exactly on size threshold
    repo.make_dirs(os.path.join("threshold", "file"))
    with repo.open_file(
        os.path.join("threshold", "file", "test.uasset"), "w+b"
    ) as output_file:
        output_file.write(os.urandom(65536))

    # Also one big file for re-fragmentation
    repo.make_dirs(os.path.join("large", "file"))
    large_file_path = os.path.join("large", "file", "test.png")
    with repo.open_file(os.path.join(large_file_path), "w+b") as output_file:
        output_file.write(os.urandom(160 * 1024 * 1024))

    # Add a copy for deduplication
    repo.copy_file(large_file_path, os.path.join("large", "file", "test2.png"))

    repo.stage(scan=True)

    # Commit the files with a low file count limit to slow it down, then kill
    # the commit mid-flight to exercise the server's behavior when a QUIC
    # connection is abruptly closed, and verify local store consistency after
    # a random process kill.
    p = subprocess.Popen(
        [
            lore_executable_path,
            "--repository",
            repo.path,
            "--file-count-limit",
            "2",
            "commit",
            "Test commit",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=repo.sandboxed_env(),
    )

    # Wait for commit to announce it has started fragmenting files, then give the
    # background upload tracker a brief moment to spawn before killing. This
    # anchors the kill inside the commit body regardless of machine speed.
    commit_started_marker = "Fragmenting files and updating tree hashes"
    commit_started = False
    start_deadline = time.monotonic() + 30
    assert p.stdout is not None
    while time.monotonic() < start_deadline:
        line = p.stdout.readline()
        if not line:
            break
        if commit_started_marker in line:
            commit_started = True
            break
    assert commit_started, (
        f"Commit did not reach {commit_started_marker!r} within 30 seconds"
    )

    time.sleep(0.2)
    p.terminate()
    p.wait(2)

    repo.commit("Test commit after waiting")
    repo.repository_verify()
    repo.push(max_connections=16)
    repository_id = repo.get_id()
    logger.info("Repository " + repository_id)

    # Clone the repository
    clone = repo.clone()

    # Verify files contents, mode and last modified timestamp
    for i in range(TOP_DIRS):
        subpath = str(i)
        for j in range(SUB_DIRS):
            subsubpath = os.path.join(subpath, str(j))
            for k in range(FILES_PER_DIR):
                filepath = os.path.join(subsubpath, str(k) + ".uasset")
                assert repo.compare_file(clone, filepath), (
                    f'Path "{filepath}" does not match in cloned repository'
                )

    filepath = os.path.join("threshold", "file", "test.uasset")
    assert repo.compare_file(clone, filepath), (
        f'Path "{filepath}" does not match in cloned repository'
    )

    filepath = os.path.join("large", "file", "test.png")
    assert repo.compare_file(clone, filepath), (
        f'Path "{filepath}" does not match in cloned repository'
    )

    filepath = os.path.join("large", "file", "test2.png")
    assert repo.compare_file(clone, filepath), (
        f'Path "{filepath}" does not match in cloned repository'
    )

    clone.repository_verify()

    # Delete random source files and some directories, in the proportions the
    # test used at ten top directories: about 60% of files and 20% of directories.
    dir_count = TOP_DIRS * SUB_DIRS
    for i in range(dir_count * FILES_PER_DIR * 6 // 10):
        fileint = random.randint(0, FILES_PER_DIR)
        dirint = random.randint(0, dir_count)
        path = os.path.join(
            str(dirint // SUB_DIRS), str(dirint % SUB_DIRS), str(fileint) + ".uasset"
        )
        # noinspection PyBroadException
        try:
            repo.remove_file(path)
        except:
            pass

    for i in range(dir_count // 5):
        dirint = random.randint(0, dir_count)
        path = os.path.join(str(dirint // SUB_DIRS), str(dirint % SUB_DIRS))
        shutil.rmtree(os.path.join(repo.path, path), ignore_errors=True)

    repo.stage(scan=True)
    repo.commit("Delete commit")
    repo.repository_verify()
    repo.push()

    # Sync the cloned repository
    clone.sync()

    for i in range(TOP_DIRS):
        subpath = str(i)
        assert repo.path_exists(subpath) == clone.path_exists(subpath), (
            "Source directory does not match clone directory"
        )
        for j in range(SUB_DIRS):
            subsubpath = os.path.join(subpath, str(j))
            assert repo.path_exists(subsubpath) == clone.path_exists(subsubpath), (
                "Source directory does not match clone directory"
            )
            for k in range(FILES_PER_DIR):
                filepath = os.path.join(subsubpath, str(k) + ".uasset")
                assert repo.path_exists(filepath) == clone.path_exists(filepath), (
                    "Source directory does not match clone directory"
                )

    # Verify the repository
    clone.repository_verify()

    # Create a new clone
    clone = repo.clone(offline=True)

    # Ensure we can commit offline directly after a clone
    with clone.open_file(large_file_path, "w+b") as output_file:
        output_file.write(os.urandom(160 * 1024))

    clone.stage(large_file_path, offline=True)

    # Commit the files
    clone.commit("Test commit offline after clone", offline=True)
    clone.push()
    clone.sync("@1")
    clone.branch_reset("@1")

    with clone.open_file(large_file_path, "w+b") as output_file:
        output_file.write(os.urandom(170 * 1024))

    clone.stage(large_file_path, offline=True)
    clone.commit("Test commit offline after sync", offline=True)

    assert_all_index_entries_reachable(repo)
    assert_all_index_entries_reachable(clone)
