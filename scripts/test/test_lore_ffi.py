# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
import pytest
from lore import Lore

from lore_ffi import (
    FILE_NOT_FOUND,
    HEADER_PATH,
    MIRRORED_STRUCTS,
    header_enum_value,
    header_struct_fields,
)

# `lore_file_audit_outcome_t`, which the records carry as an integer.
OUTCOME_ATTRIBUTED = 0


@pytest.mark.smoke
@pytest.mark.parametrize(
    "struct_name,mirror", MIRRORED_STRUCTS, ids=[name for name, _ in MIRRORED_STRUCTS]
)
def test_the_ctypes_mirror_holds_every_field_the_header_declares(struct_name, mirror):
    """The ctypes structs in `lore_ffi` are written by hand against `lore.h`.

    A field added to the C API leaves them short, and the library then reads
    past the end of the memory a caller allocated for one — a segfault inside
    the call, in whichever test happened to make it, pointing nowhere near the
    struct that caused it. Comparing the two here names the missing field
    instead.
    """
    if not HEADER_PATH.is_file():
        pytest.skip(f"no generated header to check against at {HEADER_PATH}")

    mirrored = [name for name, _type in mirror._fields_]

    assert mirrored == header_struct_fields(struct_name), (
        f"{mirror.__name__} no longer mirrors {struct_name}. Update its "
        f"_fields_ to match the header, in declaration order."
    )


@pytest.mark.smoke
def test_the_file_audit_event_tag_is_declared():
    """A tag carries no explicit value, so its value is its position in
    `lore_event_id_t` and a tag added ahead of it moves it. A consumer reading
    the wrong union member reads another event's bytes as a range, so the tag
    the driver reads is taken from the header rather than restated."""
    if not HEADER_PATH.is_file():
        pytest.skip(f"no generated header to check against at {HEADER_PATH}")

    audit = header_enum_value("lore_event_id_t", "LORE_EVENT_FILE_AUDIT")
    complete = header_enum_value("lore_event_id_t", "LORE_EVENT_COMPLETE")

    assert audit != complete, "each event has its own tag"
    assert audit > 0


@pytest.mark.smoke
def test_file_audit_through_the_c_api_emits_one_record_per_range(
    new_lore_repo, lore_library_path
):
    """`lore_file_audit` is the entry point an editor integration reaches
    attribution through: one `LORE_EVENT_FILE_AUDIT` event per range of lines,
    then the standard completion event, with no terminal output to parse.

    The struct is laid out by the caller here, so the field order and widths are
    the caller's to get right and the library reads them out of memory the caller
    allocated.
    """
    repo: Lore = new_lore_repo("FileAuditCapi")

    audited = "audited.txt"

    repo.write_commit_push(
        "First commit",
        {audited: ["alpha\n", "bravo\n", "charlie\n"]},
        offline=True,
    )
    first = repo.revision_info().signature

    repo.write_commit_push(
        "Second commit",
        {audited: ["alpha\n", "BRAVO\n", "charlie\n"]},
        offline=True,
    )
    second = repo.revision_info().signature

    code, events = repo.file_audit_capi(lore_library_path, audited)
    assert code == 0, f"the audit succeeded: {events}"

    ranges = [event for event in events if "lineFirst" in event]
    completions = [event for event in events if "complete" in event]

    # Ranges arrive as the walk resolves them, so they are compared in line order.
    assert sorted(
        (event["lineFirst"], event["lineLast"], event["outcome"], event["revision"])
        for event in ranges
    ) == [
        (1, 1, OUTCOME_ATTRIBUTED, first),
        (2, 2, OUTCOME_ATTRIBUTED, second),
        (3, 3, OUTCOME_ATTRIBUTED, first),
    ]
    assert {event["repository"] for event in ranges} == {repo.get_id()}, ranges

    # One completion event, carrying the success the call returned.
    assert [(event["complete"], event["errorCode"]) for event in completions] == [
        (0, 0)
    ]


@pytest.mark.smoke
def test_file_audit_through_the_c_api_reports_the_real_error_code(
    new_lore_repo, lore_library_path
):
    """A failing call reports the failure's own FFI code, on the completion event
    and as the return value, as ADR-00017 requires of every call. A path no
    revision holds is a not-found path."""
    repo: Lore = new_lore_repo("FileAuditCapiError")

    repo.write_commit_push(
        "First commit",
        {"audited.txt": ["alpha\n"]},
        offline=True,
    )

    code, events = repo.file_audit_capi(lore_library_path, "never-existed.txt")

    assert code == FILE_NOT_FOUND, f"the call returns the real error code: {events}"
    assert not [event for event in events if "lineFirst" in event], (
        f"a refused audit reports no range: {events}"
    )
    completions = [event for event in events if "complete" in event]
    assert [(event["complete"], event["errorCode"]) for event in completions] == [
        (FILE_NOT_FOUND, FILE_NOT_FOUND)
    ], f"the completion event carries the real code, not a flat 1: {events}"


@pytest.mark.smoke
def test_file_audit_through_the_c_api_lets_a_caller_stop_reading_early(
    new_lore_repo, lore_library_path
):
    """Each range is handed over as soon as the step that resolves it ends, so a
    caller that has what it needs may stop without waiting for the walk to reach
    the end of the chain.

    The most recently changed range arrives first. In this history the newest
    revision wrote line 1, while line 4 was last written by the revision that
    added the file, nine revisions back. The range over line 1 is therefore
    handed over at the first step of a nine-step walk.

    The driver ends the process from inside the callback on that first range. The
    record it already received stands, and no completion event follows it — which
    is what a caller walking away mid-walk looks like from the library's side.
    """
    repo: Lore = new_lore_repo("FileAuditCapiStop")

    audited = "audited.txt"

    def lines(first: str, second: str, churn: str) -> list[str]:
        return [f"{first}\n", f"{second}\n", f"{churn}\n", "tail\n"]

    repo.write_commit_push(
        "First commit",
        {audited: lines("alpha", "bravo", "churn")},
        offline=True,
    )

    # Six revisions that touch the third line alone, so the walk has a chain to
    # follow after the lines above it have resolved.
    for index in range(6):
        repo.write_commit_push(
            f"Churn {index}",
            {audited: lines("alpha", "bravo", f"churn {index}")},
            offline=True,
        )

    repo.write_commit_push(
        "Rewrite the second line",
        {audited: lines("alpha", "BRAVO", "churn 5")},
        offline=True,
    )
    repo.write_commit_push(
        "Rewrite the first line",
        {audited: lines("ALPHA", "BRAVO", "churn 5")},
        offline=True,
    )

    code, events = repo.file_audit_capi(lore_library_path, audited, stop_after=1)

    assert code == 0, f"the process exited on the caller's own terms: {events}"
    ranges = [event for event in events if "lineFirst" in event]
    assert len(ranges) == 1, f"the caller kept the one range it read: {events}"
    assert (ranges[0]["lineFirst"], ranges[0]["lineLast"]) == (1, 1), (
        f"the newest revision wrote line 1, so its range arrives first, before "
        f"the walk reaches the revision that wrote the bottom: {events}"
    )
    assert not [event for event in events if "complete" in event], (
        f"the caller stopped before the walk finished, so no completion event "
        f"arrived: {events}"
    )
