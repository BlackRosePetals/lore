// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_base::error::AddressNotFound;
use lore_base::error::Disconnected;
use lore_base::error::InvalidPath;
use lore_base::error::NoRemote;
use lore_base::error::NotAuthenticated;
use lore_base::error::NotAuthorized;
use lore_base::error::NotConnected;
use lore_base::error::NotFound;
use lore_base::error::PayloadNotFound;
use lore_base::types::Hash;
use lore_revision::file::audit::*;
use lore_revision::lore::RepositoryId;
use lore_revision::util::path::RelativePath;

fn revision(byte: u8) -> Hash {
    Hash::from([byte; 32])
}

/// The rules an audit compares under unless the caller changes them.
fn default_whitespace() -> Whitespace {
    Whitespace {
        ignore_space_at_eol: true,
        ignore_space_change: false,
    }
}

fn literal_whitespace() -> Whitespace {
    Whitespace {
        ignore_space_at_eol: false,
        ignore_space_change: false,
    }
}

/// A repository other than the one the audit started in.
fn linked_repository() -> RepositoryId {
    RepositoryId::from([5; 16])
}

fn source(repository: RepositoryId, byte: u8) -> RevisionSource {
    RevisionSource {
        repository,
        revision: revision(byte),
    }
}

fn uncommitted() -> LineOutcome {
    LineOutcome::Other(LoreFileAuditOutcome::Uncommitted)
}

fn range(first: u32, last: u32, outcome: LineOutcome) -> AuditRange {
    AuditRange {
        first,
        last,
        outcome,
    }
}

fn audited_path() -> RelativePath {
    RelativePath::new_from_initial_path("audited.txt").expect("a clean relative path")
}

fn unattributed(count: u32) -> Vec<UnattributedLine> {
    (1..=count)
        .map(|line| UnattributedLine {
            audited: line,
            compared: line,
        })
        .collect()
}

#[test]
fn without_a_working_copy_every_line_of_the_revision_is_the_walk_s() {
    let (unattributed, uncommitted) = initial_lines("a\nb\n", None, default_whitespace());

    assert_eq!(
        unattributed,
        vec![
            UnattributedLine {
                audited: 1,
                compared: 1
            },
            UnattributedLine {
                audited: 2,
                compared: 2
            },
        ]
    );
    assert!(uncommitted.is_empty());
}

#[test]
fn a_working_copy_holding_the_revision_s_content_claims_no_line() {
    let (unattributed, uncommitted) = initial_lines("a\nb\n", Some("a\nb\n"), default_whitespace());

    assert_eq!(
        unattributed,
        vec![
            UnattributedLine {
                audited: 1,
                compared: 1
            },
            UnattributedLine {
                audited: 2,
                compared: 2
            },
        ]
    );
    assert!(uncommitted.is_empty());
}

#[test]
fn a_line_differing_on_disk_is_uncommitted_and_leaves_the_walk() {
    // Line 2 was edited and line 4 added, so the file on disk has four lines
    // where the revision has three.
    let (unattributed, uncommitted_ranges) =
        initial_lines("a\nb\nc\n", Some("a\nB\nc\ne\n"), default_whitespace());

    // The audited numbers are the file on disk's; the compared numbers are
    // the revision's, so the walk carries line 3 of disk onto line 3 of the
    // revision.
    assert_eq!(
        unattributed,
        vec![
            UnattributedLine {
                audited: 1,
                compared: 1
            },
            UnattributedLine {
                audited: 3,
                compared: 3
            },
        ]
    );
    // The two lines are not consecutive, so they are two ranges.
    assert_eq!(
        uncommitted_ranges,
        vec![range(2, 2, uncommitted()), range(4, 4, uncommitted())]
    );
}

#[test]
fn consecutive_lines_differing_on_disk_are_one_uncommitted_range() {
    let (unattributed, uncommitted_ranges) =
        initial_lines("a\nb\nc\n", Some("x\ny\nc\n"), default_whitespace());

    assert_eq!(
        unattributed,
        vec![UnattributedLine {
            audited: 3,
            compared: 3
        }]
    );
    assert_eq!(uncommitted_ranges, vec![range(1, 2, uncommitted())]);
}

#[test]
fn a_line_deleted_on_disk_claims_nothing_and_renumbers_the_rest() {
    // Removing a line leaves every surviving line with the revision that
    // wrote it, so nothing is uncommitted and the two numberings part
    // company: line 2 on disk is line 3 of the revision.
    let (unattributed, uncommitted) =
        initial_lines("a\nb\nc\ne\n", Some("a\nc\ne\n"), default_whitespace());

    assert_eq!(
        unattributed,
        vec![
            UnattributedLine {
                audited: 1,
                compared: 1
            },
            UnattributedLine {
                audited: 2,
                compared: 3
            },
            UnattributedLine {
                audited: 3,
                compared: 4
            },
        ]
    );
    assert!(uncommitted.is_empty());
}

#[test]
fn a_deletion_and_an_addition_on_disk_number_the_lines_between_them() {
    // Line 2 of the revision was removed and a line added after what is now
    // line 2 on disk, so the added line is uncommitted and the lines on
    // either side of it sit at different numbers on the two sides.
    let (unattributed, uncommitted_ranges) =
        initial_lines("a\nb\nc\ne\n", Some("a\nc\nx\ne\n"), default_whitespace());

    assert_eq!(
        unattributed,
        vec![
            UnattributedLine {
                audited: 1,
                compared: 1
            },
            UnattributedLine {
                audited: 2,
                compared: 3
            },
            UnattributedLine {
                audited: 4,
                compared: 4
            },
        ]
    );
    assert_eq!(uncommitted_ranges, vec![range(3, 3, uncommitted())]);
}

#[test]
fn a_line_inserted_on_disk_shifts_the_lines_after_it() {
    let (unattributed, uncommitted_ranges) =
        initial_lines("a\nb\n", Some("a\nnew\nb\n"), default_whitespace());

    assert_eq!(
        unattributed,
        vec![
            UnattributedLine {
                audited: 1,
                compared: 1
            },
            UnattributedLine {
                audited: 3,
                compared: 2
            },
        ]
    );
    assert_eq!(uncommitted_ranges, vec![range(2, 2, uncommitted())]);
}

/// The walk read through to the remote, so a version neither store holds
/// was removed rather than never fetched.
fn remote() -> Reachability {
    Reachability {
        linked: false,
        offline: false,
    }
}

/// The walk read the local store alone, so a version it did not find says
/// nothing about whether the version exists.
fn local_only() -> Reachability {
    Reachability {
        linked: false,
        offline: true,
    }
}

#[test]
fn a_missing_payload_is_an_obliterated_version() {
    assert_eq!(
        unresolved_reason(&PayloadNotFound { hash: [0; 32] }.into(), remote()),
        Some(LoreFileAuditOutcome::Obliterated)
    );
    assert_eq!(
        unresolved_reason(&AddressNotFound { address: [0; 48] }.into(), remote()),
        Some(LoreFileAuditOutcome::Obliterated)
    );
    assert_eq!(
        unresolved_reason(&NotFound.into(), remote()),
        Some(LoreFileAuditOutcome::Obliterated)
    );
}

/// Offline the read never reaches the remote, so a payload merely absent
/// locally raises the same error an obliterated one does. Reporting it as
/// obliterated would name a removal that never happened, so the reason is
/// the one the design gives for content out of reach.
#[test]
fn a_missing_payload_offline_is_content_out_of_reach_rather_than_obliterated() {
    for error in [
        AuditError::from(PayloadNotFound { hash: [0; 32] }),
        AuditError::from(AddressNotFound { address: [0; 48] }),
        AuditError::from(NotFound),
    ] {
        assert_eq!(
            unresolved_reason(&error, local_only()),
            Some(LoreFileAuditOutcome::ContentUnavailable),
            "offline, {error} must not read as a removal",
        );
    }
}

/// Inside a link the repository the chain continues in is the thing out of
/// reach, so the gap is reported against the link whichever way the read
/// failed.
#[test]
fn a_missing_payload_offline_inside_a_link_is_reported_against_the_link() {
    assert_eq!(
        unresolved_reason(
            &PayloadNotFound { hash: [0; 32] }.into(),
            Reachability {
                linked: true,
                offline: true,
            }
        ),
        Some(LoreFileAuditOutcome::LinkUnavailable)
    );

    // With a remote to read through, the same failure inside a link is a
    // removal, exactly as it is outside one.
    assert_eq!(
        unresolved_reason(
            &PayloadNotFound { hash: [0; 32] }.into(),
            Reachability {
                linked: true,
                offline: false,
            }
        ),
        Some(LoreFileAuditOutcome::Obliterated)
    );
}

fn not_connected() -> AuditError {
    NotConnected {
        reason: "the remote went away".to_string(),
    }
    .into()
}

/// A lost connection is a failure of the environment, not an answer about
/// the file, so it ends the audit with its error rather than naming a reason
/// for the lines still unattributed.
#[test]
fn a_lost_connection_ends_the_audit() {
    for reach in [
        remote(),
        local_only(),
        Reachability {
            linked: true,
            offline: false,
        },
    ] {
        assert_eq!(unresolved_reason(&Disconnected.into(), reach), None);
        assert_eq!(unresolved_reason(&not_connected(), reach), None);
    }

    assert!(matches!(
        gap::<()>(Err(Disconnected.into()), remote()),
        Err(AuditError::Disconnected(_))
    ));
    assert!(matches!(
        gap::<()>(Err(not_connected()), remote()),
        Err(AuditError::NotConnected(_))
    ));
}

/// A read that succeeded passes through, and a gap the design names becomes
/// the reason the walk stops with.
#[test]
fn a_read_either_yields_its_value_or_the_reason_it_stops_the_walk() {
    assert!(matches!(gap(Ok(3), remote()), Ok(Ok(3))));
    assert!(matches!(
        gap::<()>(Err(PayloadNotFound { hash: [0; 32] }.into()), remote()),
        Ok(Err(LoreFileAuditOutcome::Obliterated))
    ));
    assert!(matches!(
        gap::<()>(Err(NoRemote.into()), local_only()),
        Ok(Err(LoreFileAuditOutcome::ContentUnavailable))
    ));
}

/// An instance with no remote reads its local store by configuration, so
/// content it does not hold is out of reach rather than a failure.
#[test]
fn content_with_no_remote_to_read_through_is_unavailable() {
    assert_eq!(
        unresolved_reason(&NoRemote.into(), remote()),
        Some(LoreFileAuditOutcome::ContentUnavailable)
    );
    assert_eq!(
        unresolved_reason(
            &NoRemote.into(),
            Reachability {
                linked: true,
                offline: false,
            }
        ),
        Some(LoreFileAuditOutcome::LinkUnavailable)
    );
}

#[test]
fn a_linked_repository_refusing_the_caller_is_reported_against_the_link() {
    let linked = Reachability {
        linked: true,
        offline: false,
    };
    assert_eq!(
        unresolved_reason(&NotAuthorized.into(), linked),
        Some(LoreFileAuditOutcome::LinkUnavailable)
    );
    assert_eq!(
        unresolved_reason(&NotAuthenticated.into(), linked),
        Some(LoreFileAuditOutcome::LinkUnavailable)
    );
}

#[test]
fn a_failure_the_design_does_not_account_for_stays_a_failure() {
    // Refused in the repository the audit started in, where no link stands
    // between the walk and the content.
    assert_eq!(unresolved_reason(&NotAuthorized.into(), remote()), None);
    assert_eq!(unresolved_reason(&NotAuthorized.into(), local_only()), None);
    assert_eq!(
        unresolved_reason(
            &InvalidPath {
                path: "a".to_string()
            }
            .into(),
            Reachability {
                linked: true,
                offline: true,
            }
        ),
        None
    );
}

#[test]
fn trailing_whitespace_alone_leaves_a_line_with_the_revision_that_wrote_it() {
    assert_eq!(
        unchanged_line_origins("a\nb\n", "a\nb   \n", default_whitespace()),
        vec![Some(1), Some(2)]
    );
    // A line ending changing from LF to CRLF is trailing whitespace too.
    assert_eq!(
        unchanged_line_origins("a\nb\n", "a\r\nb\r\n", default_whitespace()),
        vec![Some(1), Some(2)]
    );
}

#[test]
fn a_literal_comparison_counts_trailing_whitespace_as_a_change() {
    assert_eq!(
        unchanged_line_origins("a\nb\n", "a\nb   \n", literal_whitespace()),
        vec![Some(1), None]
    );
}

#[test]
fn a_line_ending_counts_as_a_change_when_compared_literally() {
    assert_eq!(
        unchanged_line_origins("a\nb\n", "a\r\nb\r\n", literal_whitespace()),
        vec![None, None]
    );
}

#[test]
fn a_final_newline_added_or_removed_changes_the_last_line() {
    // The terminator is part of the line it ends, and no whitespace rule
    // removes it, so the last line with content takes the change.
    assert_eq!(
        unchanged_line_origins("a\nb", "a\nb\n", default_whitespace()),
        vec![Some(1), None]
    );
    assert_eq!(
        unchanged_line_origins("a\nb\n", "a\nb", default_whitespace()),
        vec![Some(1), None]
    );
}

#[test]
fn an_empty_line_added_at_the_end_is_the_only_change() {
    assert_eq!(
        unchanged_line_origins("a\nb\n", "a\nb\n\n", default_whitespace()),
        vec![Some(1), Some(2), None]
    );
}

#[test]
fn a_file_ending_lines_with_cr_alone_is_one_line() {
    // Lines end at `\n` only, so a carriage return alone does not end one.
    assert_eq!(
        unchanged_line_origins(
            "a\nb\n",
            "a\rb\r",
            Whitespace {
                ignore_space_at_eol: true,
                ignore_space_change: true,
            }
        ),
        vec![None]
    );
}

#[test]
fn reindentation_counts_unless_internal_whitespace_is_ignored() {
    // A diff describes one change literally; attribution asks who wrote a
    // line, and reindenting it is a change to the line either way unless
    // the caller says otherwise.
    assert_eq!(
        unchanged_line_origins("  a\n", "      a\n", default_whitespace()),
        vec![None]
    );
    assert_eq!(
        unchanged_line_origins(
            "  a\n",
            "      a\n",
            Whitespace {
                ignore_space_at_eol: true,
                ignore_space_change: true,
            }
        ),
        vec![Some(1)]
    );
}

#[test]
fn a_line_touched_only_in_trailing_whitespace_on_disk_is_not_uncommitted() {
    let (unattributed, uncommitted_ranges) =
        initial_lines("a\nb\n", Some("a\nb  \n"), default_whitespace());

    assert_eq!(
        unattributed,
        vec![
            UnattributedLine {
                audited: 1,
                compared: 1
            },
            UnattributedLine {
                audited: 2,
                compared: 2
            },
        ]
    );
    assert!(uncommitted_ranges.is_empty());

    // Under a literal comparison the same edit is an uncommitted line.
    let (unattributed, uncommitted_ranges) =
        initial_lines("a\nb\n", Some("a\nb  \n"), literal_whitespace());
    assert_eq!(
        unattributed,
        vec![UnattributedLine {
            audited: 1,
            compared: 1
        }]
    );
    assert_eq!(uncommitted_ranges, vec![range(2, 2, uncommitted())]);
}

#[test]
fn line_count_counts_an_unterminated_last_line() {
    assert_eq!(line_count(""), 0);
    assert_eq!(line_count("a\n"), 1);
    assert_eq!(line_count("a\nb"), 2);
    assert_eq!(line_count("a\nb\n"), 2);
}

#[test]
fn identical_versions_carry_every_line() {
    let origins = unchanged_line_origins("a\nb\nc\n", "a\nb\nc\n", default_whitespace());
    assert_eq!(origins, vec![Some(1), Some(2), Some(3)]);
}

#[test]
fn a_changed_line_has_no_origin() {
    let origins = unchanged_line_origins("a\nb\nc\n", "a\nB\nc\n", default_whitespace());
    assert_eq!(origins, vec![Some(1), None, Some(3)]);
}

#[test]
fn lines_after_an_insertion_shift_onto_their_own_origins() {
    let origins = unchanged_line_origins("a\nb\n", "a\nx\ny\nb\n", default_whitespace());
    assert_eq!(origins, vec![Some(1), None, None, Some(2)]);
}

#[test]
fn lines_after_a_deletion_shift_onto_their_own_origins() {
    let origins = unchanged_line_origins("a\nb\nc\ne\n", "a\ne\n", default_whitespace());
    assert_eq!(origins, vec![Some(1), Some(4)]);
}

#[test]
fn every_line_of_a_new_file_has_no_origin() {
    let origins = unchanged_line_origins("", "a\nb\n", default_whitespace());
    assert_eq!(origins, vec![None, None]);
}

#[test]
fn an_emptied_file_yields_no_origins() {
    assert_eq!(
        unchanged_line_origins("a\nb\n", "", default_whitespace()),
        Vec::<Option<u32>>::new()
    );
}

#[test]
fn a_final_line_gaining_a_terminator_is_changed() {
    let origins = unchanged_line_origins("a\nb", "a\nb\n", default_whitespace());
    assert_eq!(origins, vec![Some(1), None]);
}

#[test]
fn a_step_claims_changed_lines_and_carries_the_rest() {
    let mut unattributed = unattributed(3);
    let claimed = source(RepositoryId::default(), 7);

    assert_eq!(
        attribute_step(&mut unattributed, &[Some(1), None, Some(3)], claimed),
        vec![range(2, 2, LineOutcome::Revision(claimed))]
    );
    assert_eq!(
        unattributed,
        vec![
            UnattributedLine {
                audited: 1,
                compared: 1
            },
            UnattributedLine {
                audited: 3,
                compared: 3
            },
        ]
    );
}

#[test]
fn a_carried_line_keeps_its_audited_number_across_steps() {
    let mut unattributed = unattributed(2);
    let first = source(RepositoryId::default(), 1);
    let second = source(RepositoryId::default(), 2);

    // The audited line 2 came from line 5 of the previous version.
    assert_eq!(
        attribute_step(&mut unattributed, &[None, Some(5)], first),
        vec![range(1, 1, LineOutcome::Revision(first))]
    );
    // That version changed line 5, so the audited line 2 belongs to it.
    assert_eq!(
        attribute_step(&mut unattributed, &[None, None, None, None, None], second),
        vec![range(2, 2, LineOutcome::Revision(second))]
    );
    assert!(unattributed.is_empty());
}

#[test]
fn a_step_that_changed_nothing_claims_nothing() {
    let mut unattributed = unattributed(2);

    assert_eq!(
        attribute_step(
            &mut unattributed,
            &[Some(4), Some(5)],
            source(RepositoryId::default(), 7),
        ),
        Vec::new()
    );
    assert_eq!(
        unattributed,
        vec![
            UnattributedLine {
                audited: 1,
                compared: 4
            },
            UnattributedLine {
                audited: 2,
                compared: 5
            },
        ]
    );
}

/// A step's ranges are final when the step ends, so they are complete even
/// while lines above them are still unresolved.
#[test]
fn a_step_s_ranges_are_complete_while_lines_above_them_are_unresolved() {
    let mut unattributed = unattributed(4);
    let claimed = source(RepositoryId::default(), 1);

    assert_eq!(
        attribute_step(&mut unattributed, &[Some(1), Some(2), None, None], claimed),
        vec![range(3, 4, LineOutcome::Revision(claimed))]
    );
    assert_eq!(unattributed.len(), 2);
}

#[test]
fn claiming_the_remainder_empties_the_set() {
    let mut unattributed = unattributed(2);
    let budget_exhausted = LineOutcome::Other(LoreFileAuditOutcome::BudgetExhausted);

    assert_eq!(
        claim_remaining(&mut unattributed, budget_exhausted),
        vec![range(1, 2, budget_exhausted)]
    );
    assert!(unattributed.is_empty());

    // An emptied set has nothing left to claim.
    assert_eq!(
        claim_remaining(
            &mut unattributed,
            LineOutcome::Other(LoreFileAuditOutcome::Unresolved)
        ),
        Vec::new()
    );
}

#[test]
fn consecutive_lines_sharing_an_outcome_are_one_range() {
    let only = LineOutcome::Revision(source(RepositoryId::default(), 1));
    let mut ranges = Vec::new();
    for line in 1..=3 {
        push_line(&mut ranges, line, only);
    }

    assert_eq!(ranges, vec![range(1, 3, only)]);
}

#[test]
fn a_gap_or_a_different_outcome_starts_a_new_range() {
    let first = LineOutcome::Revision(source(RepositoryId::default(), 1));
    let second = LineOutcome::Revision(source(RepositoryId::default(), 2));
    let mut ranges = Vec::new();
    push_line(&mut ranges, 1, first);
    push_line(&mut ranges, 3, first);
    push_line(&mut ranges, 4, second);

    assert_eq!(
        ranges,
        vec![range(1, 1, first), range(3, 3, first), range(4, 4, second)]
    );
}

/// Each claim groups only its own lines, so a run never spans lines claimed
/// in different steps. The design relies on no outcome being claimed in two
/// steps.
#[test]
fn a_range_never_spans_lines_claimed_in_different_steps() {
    let only = LineOutcome::Revision(source(RepositoryId::default(), 1));
    let mut unattributed = unattributed(2);
    let mut above = vec![unattributed.remove(0)];

    assert_eq!(claim_remaining(&mut above, only), vec![range(1, 1, only)]);
    assert_eq!(
        claim_remaining(&mut unattributed, only),
        vec![range(2, 2, only)]
    );
}

/// A walk that ends in an error claims every line it did not reach as
/// `unresolved`: the lines it never reached, above, between and below the
/// lines it resolved, become ranges of their own.
#[test]
fn a_failed_walk_reports_the_lines_it_did_not_reach_as_unresolved() {
    let unresolved = LineOutcome::Other(LoreFileAuditOutcome::Unresolved);

    // Lines 2, 3 and 5 were claimed by earlier steps.
    let mut unattributed = vec![
        UnattributedLine {
            audited: 1,
            compared: 1,
        },
        UnattributedLine {
            audited: 4,
            compared: 2,
        },
        UnattributedLine {
            audited: 6,
            compared: 3,
        },
        UnattributedLine {
            audited: 7,
            compared: 4,
        },
    ];

    assert_eq!(
        stop_walk(
            &mut unattributed,
            LoreFileAuditOutcome::Unresolved,
            &audited_path()
        ),
        vec![
            range(1, 1, unresolved),
            range(4, 4, unresolved),
            range(6, 7, unresolved),
        ]
    );
    assert!(unattributed.is_empty());
}

/// Ranges arrive in the order the walk claims them, the most recently changed
/// first. Taken together they cover every line of the file exactly once.
#[test]
fn every_line_of_a_walk_is_in_exactly_one_range() {
    let first = source(RepositoryId::default(), 1);
    let second = source(RepositoryId::default(), 2);
    let added = source(RepositoryId::default(), 3);

    // Line 2 is edited on disk.
    let (mut unattributed, uncommitted_ranges) = initial_lines(
        "a\nb\nc\nx\ny\n",
        Some("a\nB\nc\nx\ny\n"),
        default_whitespace(),
    );
    let mut sent = uncommitted_ranges;
    // The newest revision wrote lines 3 and 4, and lines 1 and 5 carry onto
    // lines 1 and 3 of the version before it.
    sent.extend(attribute_step(
        &mut unattributed,
        &[Some(1), Some(2), None, None, Some(3)],
        first,
    ));
    // The next revision wrote what is now line 5.
    sent.extend(attribute_step(
        &mut unattributed,
        &[Some(1), Some(2), None],
        second,
    ));
    // The revision that added the file holds the rest.
    sent.extend(claim_remaining(
        &mut unattributed,
        LineOutcome::Revision(added),
    ));

    assert_eq!(
        sent,
        vec![
            range(2, 2, uncommitted()),
            range(3, 4, LineOutcome::Revision(first)),
            range(5, 5, LineOutcome::Revision(second)),
            range(1, 1, LineOutcome::Revision(added)),
        ],
        "line 1 arrives last, after every range below it"
    );

    let mut lines: Vec<u32> = sent
        .iter()
        .flat_map(|range| range.first..=range.last)
        .collect();
    lines.sort_unstable();
    assert_eq!(lines, (1..=5).collect::<Vec<_>>());
}

/// A file metadata node names a repository only where the version before it
/// came from a linked one, so a zero is the repository the walk is in.
#[test]
fn only_a_named_other_repository_is_a_link_crossed_mid_chain() {
    let current = RepositoryId::from([1; 16]);

    assert_eq!(crossed_repository(RepositoryId::default(), current), None);
    assert_eq!(crossed_repository(current, current), None);
    assert_eq!(
        crossed_repository(linked_repository(), current),
        Some(linked_repository())
    );
}

#[test]
fn every_outcome_prints_as_the_name_it_serializes_under() {
    let named = [
        (LoreFileAuditOutcome::Attributed, "attributed"),
        (LoreFileAuditOutcome::Uncommitted, "uncommitted"),
        (LoreFileAuditOutcome::BudgetExhausted, "budget-exhausted"),
        (LoreFileAuditOutcome::Obliterated, "obliterated"),
        (LoreFileAuditOutcome::BinaryInHistory, "binary-in-history"),
        (LoreFileAuditOutcome::LinkUnavailable, "link-unavailable"),
        (
            LoreFileAuditOutcome::ContentUnavailable,
            "content-unavailable",
        ),
        (LoreFileAuditOutcome::Unresolved, "unresolved"),
    ];

    for (outcome, name) in named {
        assert_eq!(outcome.as_string(), name);
        assert_eq!(
            serde_json::to_string(&outcome).expect("serializing an outcome"),
            format!("\"{name}\""),
            "the summary and the JSON disagree on {outcome:?}",
        );
    }
}

/// A C consumer branches on these values, so they do not move.
#[test]
fn every_outcome_keeps_its_c_value() {
    assert_eq!(LoreFileAuditOutcome::Attributed as i32, 0);
    assert_eq!(LoreFileAuditOutcome::Uncommitted as i32, 1);
    assert_eq!(LoreFileAuditOutcome::BudgetExhausted as i32, 2);
    assert_eq!(LoreFileAuditOutcome::Obliterated as i32, 3);
    assert_eq!(LoreFileAuditOutcome::BinaryInHistory as i32, 4);
    assert_eq!(LoreFileAuditOutcome::LinkUnavailable as i32, 5);
    assert_eq!(LoreFileAuditOutcome::ContentUnavailable as i32, 6);
    assert_eq!(LoreFileAuditOutcome::Unresolved as i32, 7);
}

#[test]
fn an_attributed_range_names_its_revision_and_lines() {
    let data = audit_event_data(
        RepositoryId::default(),
        AuditRange {
            first: 2,
            last: 5,
            outcome: LineOutcome::Revision(source(RepositoryId::default(), 7)),
        },
    );

    assert_eq!(
        data,
        LoreFileAuditEventData {
            repository: RepositoryId::default(),
            revision: revision(7),
            line_first: 2,
            line_last: 5,
            outcome: LoreFileAuditOutcome::Attributed,
        }
    );
}

/// A range no revision holds carries a zero revision, and its outcome says
/// why: uncommitted, or the reason the walk stopped.
#[test]
fn a_range_no_revision_holds_carries_a_zero_revision_and_its_outcome() {
    for (outcome, expected) in [
        (
            LineOutcome::Other(LoreFileAuditOutcome::Uncommitted),
            LoreFileAuditOutcome::Uncommitted,
        ),
        (
            LineOutcome::Other(LoreFileAuditOutcome::BudgetExhausted),
            LoreFileAuditOutcome::BudgetExhausted,
        ),
        (
            LineOutcome::Other(LoreFileAuditOutcome::Obliterated),
            LoreFileAuditOutcome::Obliterated,
        ),
    ] {
        let data = audit_event_data(
            RepositoryId::default(),
            AuditRange {
                first: 1,
                last: 1,
                outcome,
            },
        );

        assert_eq!(data.outcome, expected, "for {outcome:?}");
        assert!(data.revision.is_zero(), "for {outcome:?}");
    }
}

/// Every range names the repository its own revision came from, so a walk
/// that crossed a link does not report the whole file against one
/// repository.
#[test]
fn a_range_names_the_repository_its_revision_came_from() {
    let data = audit_event_data(
        RepositoryId::default(),
        AuditRange {
            first: 1,
            last: 3,
            outcome: LineOutcome::Revision(source(linked_repository(), 7)),
        },
    );

    assert_eq!(data.repository, linked_repository());
    assert_eq!(data.revision, revision(7));
}

/// A range no revision holds names the repository the audited version was
/// read in: no revision answered for it, and the file is the one the caller
/// asked about.
#[test]
fn a_range_no_revision_holds_names_the_audited_repository() {
    for outcome in [
        LineOutcome::Other(LoreFileAuditOutcome::Uncommitted),
        LineOutcome::Other(LoreFileAuditOutcome::LinkUnavailable),
    ] {
        let data = audit_event_data(
            linked_repository(),
            AuditRange {
                first: 1,
                last: 1,
                outcome,
            },
        );

        assert_eq!(data.repository, linked_repository(), "for {outcome:?}");
    }
}

/// Two revisions of one hash in two repositories are two answers: the
/// ranges stay apart, and each names its own repository.
#[test]
fn one_revision_hash_in_two_repositories_is_two_answers() {
    let audited = source(RepositoryId::default(), 7);
    let linked = source(linked_repository(), 7);

    let mut ranges = Vec::new();
    push_line(&mut ranges, 1, LineOutcome::Revision(audited));
    push_line(&mut ranges, 2, LineOutcome::Revision(linked));

    let reported: Vec<(RepositoryId, Hash)> = ranges
        .into_iter()
        .map(|range| {
            let data = audit_event_data(RepositoryId::default(), range);
            (data.repository, data.revision)
        })
        .collect();

    assert_eq!(
        reported,
        vec![
            (RepositoryId::default(), revision(7)),
            (linked_repository(), revision(7)),
        ]
    );
}

/// The JSON record carries the range and its outcome, and nothing a caller
/// already knows or fetches from the revision itself.
#[test]
fn the_json_record_carries_the_range_alone() {
    let json = serde_json::to_value(audit_event_data(
        RepositoryId::default(),
        AuditRange {
            first: 1,
            last: 3,
            outcome: LineOutcome::Revision(source(RepositoryId::default(), 7)),
        },
    ))
    .expect("serializing a range");

    let mut keys: Vec<&str> = json
        .as_object()
        .expect("a range serializes as an object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        vec!["lineFirst", "lineLast", "outcome", "repository", "revision"]
    );
    assert_eq!(json["outcome"], serde_json::json!("attributed"));
    assert_eq!(json["revision"], serde_json::json!(revision(7).to_string()));
}
