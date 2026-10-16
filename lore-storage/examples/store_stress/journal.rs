// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The journal of a store directory: every operation `work` starts, and how each ended, one line
//! each, appended with a single write so that a process killed at any point leaves whole lines,
//! in the order they happened.
//!
//! `<gen> s <id> <op> <key> <variant>` starts operation `id` of generation `gen`, one process run;
//! `<gen> a <id>` records its success, and `<gen> x <id>` its failure, `x <id> nf` one that found
//! nothing to act on. `<gen> s <id> obc <key> <variant> <list>` starts the obliterate of a
//! fragment of `list` that obliterating the list carries out. `<gen> fs <id> <sync> <strong>`
//! starts a flush, `fe <id>` ends one that succeeded, `fx <id>` one that failed. `<gen> es <id>`
//! starts an eviction pass and `ee <id> <removed>` ends it; `cs <id>` and `ce <id>` do the same for
//! a compaction pass, whose size eviction may take payloads. `<gen> g gc-unseen` records that
//! the generation collects garbage where the journal does not see it. `<gen> v ...` records a
//! violation seen online.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

pub const JOURNAL_FILE: &str = "stress.journal";
const GENERATION_FILE: &str = "stress.gen";

pub struct Journal {
    file: std::fs::File,
    pub generation: u64,
    next_id: AtomicU64,
    /// Whether a line failed to land, as on a full disk, which leaves the history incomplete.
    lost: AtomicBool,
}

impl Journal {
    /// Open the journal of `dir` for a new generation.
    pub fn open(dir: &Path) -> std::io::Result<Journal> {
        let generation_path = dir.join(GENERATION_FILE);
        let generation = std::fs::read_to_string(&generation_path)
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok())
            .unwrap_or(0)
            + 1;
        std::fs::write(&generation_path, generation.to_string())?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(JOURNAL_FILE))?;
        Ok(Journal {
            file,
            generation,
            next_id: AtomicU64::new(1),
            lost: AtomicBool::new(false),
        })
    }

    pub fn log(&self, args: std::fmt::Arguments<'_>) {
        let mut line = String::with_capacity(64);
        let _ = writeln!(line, "{} {args}", self.generation);
        if (&self.file).write_all(line.as_bytes()).is_err()
            && !self.lost.swap(true, Ordering::Relaxed)
        {
            // A history missing a line would judge the store by operations it does not list.
            eprintln!("JOURNAL-LOST: a journal write failed; this run's history is incomplete");
        }
    }

    fn next(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Record the start of an operation, returning its id for [`Journal::ack`].
    pub fn start(&self, op: &str, key: u64, variant: u32) -> u64 {
        let id = self.next();
        self.log(format_args!("s {id} {op} {key} {variant}"));
        id
    }

    /// Record the start of the obliterate of `key`, a fragment of `list`, that obliterating the
    /// list carries out.
    pub fn start_child(&self, key: u64, variant: u32, list: u64) -> u64 {
        let id = self.next();
        self.log(format_args!("s {id} obc {key} {variant} {list}"));
        id
    }

    pub fn ack(&self, id: u64) {
        self.log(format_args!("a {id}"));
    }

    /// Record the failure of an operation, `not_found` when it found nothing to act on.
    pub fn fail(&self, id: u64, not_found: bool) {
        if not_found {
            self.log(format_args!("x {id} nf"));
        } else {
            self.log(format_args!("x {id}"));
        }
    }

    /// Record the start of a flush, `strong` when no other bucket write can be running, so that
    /// it covers every operation acknowledged before it.
    pub fn flush_start(&self, sync: bool, strong: bool) -> u64 {
        let id = self.next();
        self.log(format_args!(
            "fs {id} {} {}",
            u8::from(sync),
            u8::from(strong)
        ));
        id
    }

    pub fn flush_end(&self, id: u64, ok: bool) {
        if ok {
            self.log(format_args!("fe {id}"));
        } else {
            self.log(format_args!("fx {id}"));
        }
    }

    /// Record the start of an eviction pass, `compaction` for one that evicts by size first.
    pub fn gc_start(&self, compaction: bool) -> u64 {
        let id = self.next();
        let kind = if compaction { "cs" } else { "es" };
        self.log(format_args!("{kind} {id}"));
        id
    }

    /// Record the end of a pass, with how many entries an eviction pass removed.
    pub fn gc_end(&self, id: u64, compaction: bool, removed: u64) {
        if compaction {
            self.log(format_args!("ce {id}"));
        } else {
            self.log(format_args!("ee {id} {removed}"));
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Put,
    PutForce,
    PutMeta,
    CopyTo,
    Obliterate,
    /// The obliterate of a fragment that obliterating its list carries out, if the list is live.
    ObliterateChild,
}

impl Op {
    fn parse(text: &str) -> Option<Op> {
        Some(match text {
            "put" => Op::Put,
            "putf" => Op::PutForce,
            "putm" => Op::PutMeta,
            "cp" => Op::CopyTo,
            "ob" => Op::Obliterate,
            "obc" => Op::ObliterateChild,
            _ => return None,
        })
    }

    fn obliterates(self) -> bool {
        matches!(self, Op::Obliterate | Op::ObliterateChild)
    }

    fn stores_payload(self) -> bool {
        matches!(self, Op::Put | Op::PutForce)
    }
}

#[derive(Clone, Debug)]
pub struct Event {
    pub line: usize,
    pub generation: u64,
    pub op: Op,
    pub ack: Option<usize>,
    /// The line recording its failure.
    pub failed: Option<usize>,
    /// Whether it failed having found nothing to act on.
    pub not_found: bool,
    /// For [`Op::ObliterateChild`], the list whose obliterate it is part of.
    pub list: Option<u64>,
}

impl Event {
    /// The line it ended at, when it ended in its generation.
    fn end(&self) -> Option<usize> {
        self.ack.or(self.failed)
    }

    /// Whether it may still have been running after `line`: it ended after it, or never ended in
    /// that line's generation `generation`.
    fn running_after(&self, line: usize, generation: u64) -> bool {
        match self.end() {
            Some(end) => end > line,
            None => self.generation >= generation,
        }
    }
}

/// An eviction or compaction pass.
#[derive(Clone, Debug)]
struct GcPass {
    generation: u64,
    compaction: bool,
    end: Option<usize>,
    /// How many entries an eviction pass removed, when it ended.
    removed: Option<u64>,
}

/// What a store must hold for one key and variant, from the journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    /// An entry, holding the payload when `payload`.
    Present {
        payload: bool,
    },
    Obliterated,
    Unknown,
}

#[derive(Default)]
pub struct History {
    pub events: HashMap<(u64, u32), Vec<Event>>,
    /// Per generation, the latest line starting a flush that succeeded, no other bucket write
    /// running alongside it.
    covered_before: HashMap<u64, usize>,
    /// The same for any flush, which a bucket write claimed by another may have been left out of.
    weakly_covered_before: HashMap<u64, usize>,
    passes: Vec<GcPass>,
    /// Generations that collected garbage where the journal does not see it.
    unseen_gc: HashSet<u64>,
    /// Keys an obliterate was started for, under any variant.
    pub obliterated_keys: HashSet<u64>,
    /// Whether any run healed the store, which drops entries.
    pub healed: bool,
    pub violations: Vec<String>,
    pub generations: u64,
    pub clean_ends: u64,
    pub lines: usize,
}

impl History {
    pub fn load(dir: &Path) -> std::io::Result<History> {
        let text = match std::fs::read_to_string(dir.join(JOURNAL_FILE)) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(err) => return Err(err),
        };
        let mut history = History::default();
        let mut started: HashMap<(u64, u64), ((u64, u32), usize)> = HashMap::new();
        let mut flush_starts: HashMap<(u64, u64), (usize, bool)> = HashMap::new();
        let mut pass_starts: HashMap<(u64, u64), usize> = HashMap::new();
        for (line_number, line) in text.lines().enumerate() {
            history.lines = line_number + 1;
            let mut fields = line.split(' ');
            let Some(generation) = fields.next().and_then(|g| g.parse::<u64>().ok()) else {
                continue;
            };
            history.generations = history.generations.max(generation);
            let kind = fields.next().unwrap_or_default();
            let rest: Vec<&str> = fields.collect();
            let number = |index: usize| rest.get(index).and_then(|f| f.parse::<u64>().ok());
            match kind {
                "s" => {
                    let (Some(id), Some(op), Some(key), Some(variant)) = (
                        number(0),
                        rest.get(1).and_then(|op| Op::parse(op)),
                        number(2),
                        number(3),
                    ) else {
                        if rest.get(1) == Some(&"heal") {
                            history.healed = true;
                        }
                        continue;
                    };
                    if op.obliterates() {
                        history.obliterated_keys.insert(key);
                    }
                    let slot = (key, variant as u32);
                    let events = history.events.entry(slot).or_default();
                    events.push(Event {
                        line: line_number,
                        generation,
                        op,
                        ack: None,
                        failed: None,
                        not_found: false,
                        list: number(4).filter(|_| op == Op::ObliterateChild),
                    });
                    started.insert((generation, id), (slot, events.len() - 1));
                }
                "a" | "x" => {
                    if let Some(id) = number(0)
                        && let Some((slot, index)) = started.get(&(generation, id))
                        && let Some(event) = history
                            .events
                            .get_mut(slot)
                            .and_then(|events| events.get_mut(*index))
                    {
                        if kind == "a" {
                            event.ack = Some(line_number);
                        } else {
                            event.failed = Some(line_number);
                            event.not_found = rest.get(1) == Some(&"nf");
                        }
                    }
                }
                "fs" => {
                    if let Some(id) = number(0) {
                        let strong = number(2) == Some(1);
                        flush_starts.insert((generation, id), (line_number, strong));
                    }
                }
                "fe" => {
                    if let Some(id) = number(0)
                        && let Some((start, strong)) = flush_starts.get(&(generation, id))
                    {
                        let weak = history.weakly_covered_before.entry(generation).or_default();
                        *weak = (*weak).max(*start);
                        if *strong {
                            let covered = history.covered_before.entry(generation).or_default();
                            *covered = (*covered).max(*start);
                        }
                    }
                }
                "es" | "cs" => {
                    if let Some(id) = number(0) {
                        pass_starts.insert((generation, id), history.passes.len());
                        history.passes.push(GcPass {
                            generation,
                            compaction: kind == "cs",
                            end: None,
                            removed: None,
                        });
                    }
                }
                "ee" | "ce" => {
                    if let Some(id) = number(0)
                        && let Some(index) = pass_starts.get(&(generation, id))
                        && let Some(pass) = history.passes.get_mut(*index)
                    {
                        pass.end = Some(line_number);
                        pass.removed = number(1);
                    }
                }
                "v" => history.violations.push(line.to_string()),
                "g" if rest.first() == Some(&"end") => history.clean_ends += 1,
                "g" if rest.first() == Some(&"gc-unseen") => {
                    history.unseen_gc.insert(generation);
                }
                _ => {}
            }
        }
        Ok(history)
    }

    /// What the store must hold for `key` under `variant`, whose entries eviction may take when
    /// `evictable`. Settled only by operations that succeeded before a flush that started after
    /// them and succeeded, with nothing running alongside that could undo them.
    pub fn expect(&self, key: u64, variant: u32, evictable: bool) -> Expect {
        self.expect_covered(key, variant, evictable, false, &self.covered_before)
    }

    /// As [`History::expect`], counting every flush that succeeded, including those another bucket
    /// write ran alongside.
    pub fn expect_weakly(&self, key: u64, variant: u32, evictable: bool) -> Expect {
        self.expect_covered(key, variant, evictable, false, &self.weakly_covered_before)
    }

    /// As [`History::expect`], after a compaction that may have evicted every payload it may.
    pub fn expect_after_compaction(&self, key: u64, variant: u32, evictable: bool) -> Expect {
        self.expect_covered(key, variant, evictable, true, &self.covered_before)
    }

    fn expect_covered(
        &self,
        key: u64,
        variant: u32,
        evictable: bool,
        compacted: bool,
        covered_before: &HashMap<u64, usize>,
    ) -> Expect {
        let Some(events) = self.events.get(&(key, variant)) else {
            return Expect::Unknown;
        };
        let covered = |event: &Event| {
            event.ack.is_some_and(|ack| {
                covered_before
                    .get(&event.generation)
                    .is_some_and(|start| *start >= ack)
            })
        };
        // A copy that found no source changed nothing.
        let meaningful: Vec<&Event> = events.iter().filter(|event| !event.not_found).collect();
        let Some(last) = meaningful.last() else {
            return Expect::Unknown;
        };
        if last.op.obliterates() {
            if !covered(last) {
                return Expect::Unknown;
            }
            let overlapped = meaningful[..meaningful.len() - 1]
                .iter()
                .any(|event| event.running_after(last.line, last.generation));
            return match last.op {
                _ if overlapped => Expect::Unknown,
                Op::Obliterate => Expect::Obliterated,
                _ if self.list_walked(last, variant, covered_before) => Expect::Obliterated,
                _ => Expect::Unknown,
            };
        }
        if self.healed {
            return Expect::Unknown;
        }
        // An operation is undone by an obliterate that may have run after it, and a payload also
        // by a copy, which takes its source's, or a put without the payload, which may not keep it.
        let undone_after = |add: &Event, by: &dyn Fn(Op) -> bool| {
            meaningful.iter().any(|event| {
                !std::ptr::eq(*event, add)
                    && by(event.op)
                    && (event.line > add.line || event.running_after(add.line, add.generation))
            })
        };
        // The latest acknowledged addition no obliterate may have undone. A put without the payload
        // after an obliterate may have met the tombstone, which it leaves.
        let obliterated_before = |add: &Event| {
            meaningful
                .iter()
                .any(|event| event.op.obliterates() && event.line < add.line)
        };
        let Some(added) = meaningful.iter().rev().find(|event| {
            !event.op.obliterates()
                && covered(event)
                && (event.op != Op::PutMeta || !obliterated_before(event))
                && !undone_after(event, &Op::obliterates)
        }) else {
            return Expect::Unknown;
        };
        // A pass still running once the operation started may have taken what it stored.
        let evicted_after = |event: &Event, payloads: bool| {
            self.unseen_gc
                .iter()
                .any(|generation| *generation >= event.generation)
                || self.passes.iter().any(|pass| {
                    let after = match pass.end {
                        Some(end) => end > event.line,
                        None => pass.generation >= event.generation,
                    };
                    after
                        && (if pass.compaction {
                            payloads
                        } else {
                            pass.removed != Some(0)
                        })
                })
        };
        if evictable && evicted_after(added, false) {
            return Expect::Unknown;
        }
        let payload = meaningful
            .iter()
            .rev()
            .find(|event| event.op.stores_payload() && covered(event) && event.line <= added.line);
        let payload = payload.is_some_and(|stored| {
            !undone_after(stored, &|op| {
                op.obliterates() || matches!(op, Op::CopyTo | Op::PutMeta)
            }) && !(evictable && (compacted || evicted_after(stored, true)))
        });
        Expect::Present { payload }
    }

    /// Whether an operation storing `key` under another variant than `variant` started after the
    /// last operation on `variant`, which is what lets a deduplicating store rewrite its entry.
    pub fn stored_elsewhere_after(&self, key: u64, variant: u32) -> bool {
        let Some(last) = self
            .events
            .get(&(key, variant))
            .and_then(|events| events.last())
        else {
            return false;
        };
        self.events
            .iter()
            .any(|((other_key, other_variant), events)| {
                *other_key == key
                    && *other_variant != variant
                    && events
                        .iter()
                        .any(|event| event.line > last.line && !event.op.obliterates())
            })
    }

    /// Whether an obliterate ever started for `key` under `variant`.
    pub fn obliterated(&self, key: u64, variant: u32) -> bool {
        self.events
            .get(&(key, variant))
            .is_some_and(|events| events.iter().any(|event| event.op.obliterates()))
    }

    /// Whether the list obliterate that `child` is part of walked the list's fragments. One that
    /// finds its list already tombstoned succeeds without them, so the list must have been live:
    /// never obliterated before, or stored again since, for good and with no obliterate running.
    fn list_walked(
        &self,
        child: &Event,
        variant: u32,
        covered_before: &HashMap<u64, usize>,
    ) -> bool {
        let Some(events) = child
            .list
            .and_then(|list| self.events.get(&(list, variant)))
        else {
            return false;
        };
        let before = events.partition_point(|event| event.line < child.line);
        // The list's own obliterate, journaled just before its fragments.
        let Some((own, earlier)) = events[..before].split_last() else {
            return false;
        };
        if own.op != Op::Obliterate || own.generation != child.generation {
            return false;
        }
        let earlier: Vec<&Event> = earlier.iter().filter(|event| !event.not_found).collect();
        if !earlier.iter().any(|event| event.op == Op::Obliterate) {
            return true;
        }
        if self.healed {
            return false;
        }
        let Some((stored, older)) = earlier.split_last() else {
            return false;
        };
        let Some(ack) = stored.ack else {
            return false;
        };
        if !stored.op.stores_payload() || ack > own.line {
            return false;
        }
        if stored.generation != own.generation
            && covered_before
                .get(&stored.generation)
                .is_none_or(|start| *start < ack)
        {
            return false;
        }
        // An obliterate still running when the list was stored again may tombstone it after.
        older
            .iter()
            .all(|event| !event.running_after(stored.line, stored.generation))
    }
}
