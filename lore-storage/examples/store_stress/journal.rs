// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The journal of a store directory: every operation `work` and `calls` start, and how each ended,
//! one line each, appended with a single write so that a process killed at any point leaves whole
//! lines, in the order they happened. Processes sharing a store directory append to its one
//! journal, so its line order is the order of their operations.
//!
//! `<gen> s <id> <op> <key> <variant>` starts operation `id` of generation `gen`, one store object's
//! life: a `work` run, a `calls` process that keeps its stores, or one call of a `calls` process
//! that opens them afresh. `<gen> a <id>` records its success, and `<gen> x <id>` its failure,
//! `x <id> nf` one that found nothing to act on or, for a swap, found another value. `<gen> s <id>
//! obc <key> <variant> <list>` starts the obliterate of a fragment of `list` that obliterating the
//! list carries out. `<gen> s <id> ms <key> <value>` starts storing `value` for mutable key `key`,
//! and `<gen> s <id> cas <key> <value>` swapping it from `value` to the next value.
//! `<gen> fs <id> <sync> <strong>` starts a flush of the immutable store, `fe <id>` ends one that
//! succeeded, `fx <id>` one that failed; `mfs`, `mfe` and `mfx` do the same for the mutable store.
//! `<gen> es <id>` starts an eviction pass and `ee <id> <removed>` ends it; `cs <id>` and `ce <id>`
//! do the same for a compaction pass, whose size eviction may take payloads.
//!
//! `<gen> g start pid=<pid> ...` starts a generation, with `shared` among its words when other
//! processes use the store alongside it, and `<gen> g end` ends it cleanly. `0 g dead pid=<pid>`
//! records that process `pid` was killed, ending its shared generations. A generation that is not
//! shared ends where the next one starts. `<gen> g gc-unseen` records that the generation collects
//! garbage where the journal does not see it. `<gen> v ...` records a violation seen online.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Write as _;
use std::io::Read as _;
use std::io::Seek as _;
use std::io::Write as _;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

pub const JOURNAL_FILE: &str = "stress.journal";
const GENERATION_FILE: &str = "stress.gen";

/// Which of a directory's stores a flush or a write is of.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Store {
    Immutable,
    Mutable,
}

impl Store {
    /// The kinds of the lines that start, end and fail a flush of this store.
    fn flush_lines(self) -> [&'static str; 3] {
        match self {
            Store::Immutable => ["fs", "fe", "fx"],
            Store::Mutable => ["mfs", "mfe", "mfx"],
        }
    }
}

pub struct Journal {
    dir: PathBuf,
    file: std::fs::File,
    generation: AtomicU64,
    next_id: AtomicU64,
    /// Whether a line failed to land, as on a full disk, which leaves the history incomplete.
    lost: AtomicBool,
}

impl Journal {
    /// Open the journal of `dir` for a new generation.
    pub fn open(dir: &Path) -> std::io::Result<Journal> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join(JOURNAL_FILE))?;
        let journal = Journal {
            dir: dir.to_path_buf(),
            file,
            generation: AtomicU64::new(0),
            next_id: AtomicU64::new(1),
            lost: AtomicBool::new(false),
        };
        journal.renew()?;
        Ok(journal)
    }

    /// Start a new generation, numbered past every generation of the directory, those of other
    /// processes using it now included. The number is written over the last in place, which it is
    /// never shorter than, so that a process killed as it writes leaves one or the other.
    pub fn renew(&self) -> std::io::Result<u64> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.dir.join(GENERATION_FILE))?;
        file.lock()?;
        let text = std::io::read_to_string(&file)?;
        let generation = text.trim().parse::<u64>().unwrap_or(0) + 1;
        file.write_all_at(generation.to_string().as_bytes(), 0)?;
        self.generation.store(generation, Ordering::Relaxed);
        Ok(generation)
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    pub fn log(&self, args: std::fmt::Arguments<'_>) {
        let mut line = String::with_capacity(64);
        let _ = writeln!(line, "{} {args}", self.generation());
        if (&self.file).write_all(line.as_bytes()).is_err()
            && !self.lost.swap(true, Ordering::Relaxed)
        {
            // A history missing a line would judge the store by operations it does not list.
            eprintln!("JOURNAL-LOST: a journal write failed; this run's history is incomplete");
        }
    }

    /// A number no other call on this journal returns, as each operation's id is.
    pub fn unique(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Record the start of an operation, returning its id for [`Journal::ack`].
    pub fn start(&self, op: &str, key: u64, variant: u32) -> u64 {
        self.start_with(op, key, u64::from(variant))
    }

    /// Record the start of an operation on mutable key `key` with `value`, see the module
    /// documentation.
    pub fn start_with(&self, op: &str, key: u64, value: u64) -> u64 {
        let id = self.unique();
        self.log(format_args!("s {id} {op} {key} {value}"));
        id
    }

    /// Record the start of the obliterate of `key`, a fragment of `list`, that obliterating the
    /// list carries out.
    pub fn start_child(&self, key: u64, variant: u32, list: u64) -> u64 {
        let id = self.unique();
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

    /// Record the start of a flush of `store`, `strong` when no other bucket write can be running,
    /// so that it covers every operation acknowledged before it.
    pub fn flush_start(&self, store: Store, sync: bool, strong: bool) -> u64 {
        let id = self.unique();
        self.log(format_args!(
            "{} {id} {} {}",
            store.flush_lines()[0],
            u8::from(sync),
            u8::from(strong)
        ));
        id
    }

    pub fn flush_end(&self, store: Store, id: u64, ok: bool) {
        let [_, done, failed] = store.flush_lines();
        self.log(format_args!("{} {id}", if ok { done } else { failed }));
    }

    /// Record the start of an eviction pass, `compaction` for one that evicts by size first.
    pub fn gc_start(&self, compaction: bool) -> u64 {
        let id = self.unique();
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
    /// A store of a mutable key.
    MutableStore,
    /// A compare-and-swap of a mutable key from one value to the next.
    Swap,
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
            "ms" => Op::MutableStore,
            "cas" => Op::Swap,
            _ => return None,
        })
    }

    fn obliterates(self) -> bool {
        matches!(self, Op::Obliterate | Op::ObliterateChild)
    }

    fn stores_payload(self) -> bool {
        matches!(self, Op::Put | Op::PutForce)
    }

    /// The store the operation writes to.
    fn store(self) -> Store {
        match self {
            Op::MutableStore | Op::Swap => Store::Mutable,
            _ => Store::Immutable,
        }
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
    /// Whether it failed having found nothing to act on, or for a swap another value.
    pub not_found: bool,
    /// For [`Op::ObliterateChild`], the list whose obliterate it is part of.
    pub list: Option<u64>,
    /// For [`Op::MutableStore`] the value stored, for [`Op::Swap`] the value swapped from.
    pub value: u64,
}

impl Event {
    /// The line it ended at, when it ended in its generation.
    fn end(&self) -> Option<usize> {
        self.ack.or(self.failed)
    }

    /// The value a mutable key holds after it, when it succeeded.
    fn result(&self) -> u64 {
        match self.op {
            Op::Swap => self.value + 1,
            _ => self.value,
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

/// A generation's life in the journal.
#[derive(Clone, Copy, Debug)]
struct Lifetime {
    start: usize,
    pid: Option<u32>,
    /// Whether other processes used the store alongside it.
    shared: bool,
    /// The line of its `g end`, or of the `g dead` of its process.
    end: Option<usize>,
}

/// What the store must hold for one key and variant, from the journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Expect {
    /// An entry, holding the payload when `payload`.
    Present {
        payload: bool,
    },
    Obliterated,
    Unknown,
}

/// What a mutable key must hold, from the journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Value {
    /// Nothing: no store or swap of it ever started.
    Absent,
    Is(u64),
    Unknown,
}

/// Where an operation's start line put its event.
#[derive(Clone, Copy)]
enum Target {
    Slot((u64, u32)),
    Mutable(u64),
}

/// What one read of the journal found, see [`History::update`].
#[derive(Default)]
pub struct Added {
    /// Each slot an operation started on, with the operation's generation.
    pub slots: Vec<((u64, u32), u64)>,
    /// Each mutable key an operation started on, with the operation's generation.
    pub mutable: Vec<(u64, u64)>,
    /// Each operation that wrote to a store, or may have, with its generation and whether it
    /// is known to have claimed the store to write: one that succeeded, or found nothing to act
    /// on after claiming, rather than one that started, failed, or is an eviction or compaction
    /// pass.
    pub writes: Vec<(Store, u64, bool)>,
}

#[derive(Default)]
pub struct History {
    pub events: HashMap<(u64, u32), Vec<Event>>,
    /// The stores and swaps of each mutable key.
    pub mutable: HashMap<u64, Vec<Event>>,
    /// Per generation, the latest line starting a flush of the immutable store that succeeded, no
    /// other bucket write running alongside it.
    covered_before: HashMap<u64, usize>,
    /// The same for any flush, which a bucket write claimed by another may have been left out of.
    weakly_covered_before: HashMap<u64, usize>,
    /// [`Self::covered_before`] for flushes of the mutable store.
    mutable_covered_before: HashMap<u64, usize>,
    passes: Vec<GcPass>,
    /// Generations that collected garbage where the journal does not see it.
    unseen_gc: HashSet<u64>,
    lifetimes: HashMap<u64, Lifetime>,
    /// The lines starting a generation, in order.
    starts: Vec<usize>,
    /// Keys an obliterate was started for, under any variant.
    pub obliterated_keys: HashSet<u64>,
    /// Whether any run healed the store, which drops entries.
    pub healed: bool,
    pub violations: Vec<String>,
    pub generations: u64,
    pub clean_ends: u64,
    pub lines: usize,
    /// Where the next read of the journal starts.
    offset: u64,
    started: HashMap<(u64, u64), (Target, usize)>,
    flush_starts: HashMap<(u64, u64), (usize, bool, Store)>,
    pass_starts: HashMap<(u64, u64), usize>,
}

impl History {
    pub fn load(dir: &Path) -> std::io::Result<History> {
        let mut history = History::default();
        history.update(dir)?;
        Ok(history)
    }

    /// Read the lines appended to the journal of `dir` since the last read, and return what they
    /// added. A line not yet whole is left for the next read.
    pub fn update(&mut self, dir: &Path) -> std::io::Result<Added> {
        let mut text = Vec::new();
        match std::fs::File::open(dir.join(JOURNAL_FILE)) {
            Ok(mut file) => {
                file.seek(std::io::SeekFrom::Start(self.offset))?;
                std::io::BufReader::new(file).read_to_end(&mut text)?;
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        let whole = text
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |end| end + 1);
        self.offset += whole as u64;
        let mut added = Added::default();
        for line in String::from_utf8_lossy(&text[..whole]).lines() {
            self.parse(line, &mut added);
        }
        Ok(added)
    }

    fn parse(&mut self, line: &str, added: &mut Added) {
        let line_number = self.lines;
        self.lines += 1;
        let mut fields = line.split(' ');
        let Some(generation) = fields.next().and_then(|g| g.parse::<u64>().ok()) else {
            return;
        };
        self.generations = self.generations.max(generation);
        let kind = fields.next().unwrap_or_default();
        let rest: Vec<&str> = fields.collect();
        let number = |index: usize| rest.get(index).and_then(|f| f.parse::<u64>().ok());
        match kind {
            "s" => {
                let (Some(id), Some(op), Some(key), Some(argument)) = (
                    number(0),
                    rest.get(1).and_then(|op| Op::parse(op)),
                    number(2),
                    number(3),
                ) else {
                    if rest.get(1) == Some(&"heal") {
                        self.healed = true;
                    }
                    return;
                };
                if op.obliterates() {
                    self.obliterated_keys.insert(key);
                }
                let event = Event {
                    line: line_number,
                    generation,
                    op,
                    ack: None,
                    failed: None,
                    not_found: false,
                    list: number(4).filter(|_| op == Op::ObliterateChild),
                    value: argument,
                };
                let (target, events) = if op.store() == Store::Mutable {
                    added.mutable.push((key, generation));
                    (Target::Mutable(key), self.mutable.entry(key).or_default())
                } else {
                    let slot = (key, argument as u32);
                    added.slots.push((slot, generation));
                    (Target::Slot(slot), self.events.entry(slot).or_default())
                };
                events.push(event);
                added.writes.push((op.store(), generation, false));
                self.started
                    .insert((generation, id), (target, events.len() - 1));
            }
            "a" | "x" => {
                let not_found = rest.get(1) == Some(&"nf");
                if let Some(id) = number(0)
                    && let Some(event) = self.started_event(generation, id)
                {
                    if kind == "a" {
                        event.ack = Some(line_number);
                    } else {
                        event.failed = Some(line_number);
                        event.not_found = not_found;
                    }
                    let store = event.op.store();
                    if kind == "a" || not_found {
                        added.writes.push((store, generation, true));
                    }
                }
            }
            "fs" | "mfs" => {
                if let Some(id) = number(0) {
                    let strong = number(2) == Some(1);
                    let store = match kind {
                        "fs" => Store::Immutable,
                        _ => Store::Mutable,
                    };
                    self.flush_starts
                        .insert((generation, id), (line_number, strong, store));
                }
            }
            "fe" | "mfe" => {
                if let Some(id) = number(0)
                    && let Some((start, strong, store)) =
                        self.flush_starts.get(&(generation, id)).copied()
                {
                    let cover = |before: &mut HashMap<u64, usize>| {
                        let covered = before.entry(generation).or_default();
                        *covered = (*covered).max(start);
                    };
                    match store {
                        Store::Immutable => {
                            cover(&mut self.weakly_covered_before);
                            if strong {
                                cover(&mut self.covered_before);
                            }
                        }
                        Store::Mutable if strong => cover(&mut self.mutable_covered_before),
                        Store::Mutable => {}
                    }
                }
            }
            "es" | "cs" => {
                if let Some(id) = number(0) {
                    self.pass_starts.insert((generation, id), self.passes.len());
                    self.passes.push(GcPass {
                        generation,
                        compaction: kind == "cs",
                        end: None,
                        removed: None,
                    });
                    added.writes.push((Store::Immutable, generation, false));
                }
            }
            "ee" | "ce" => {
                if let Some(id) = number(0)
                    && let Some(index) = self.pass_starts.get(&(generation, id))
                    && let Some(pass) = self.passes.get_mut(*index)
                {
                    pass.end = Some(line_number);
                    pass.removed = number(1);
                }
            }
            "v" => self.violations.push(line.to_string()),
            "g" => self.lifetime(generation, &rest, line_number),
            _ => {}
        }
    }

    /// The event the start line of operation `id` of `generation` recorded.
    fn started_event(&mut self, generation: u64, id: u64) -> Option<&mut Event> {
        let (target, index) = *self.started.get(&(generation, id))?;
        let events = match target {
            Target::Slot(slot) => self.events.get_mut(&slot),
            Target::Mutable(key) => self.mutable.get_mut(&key),
        };
        events?.get_mut(index)
    }

    /// Take a `g` line: a generation starting, ending, its process killed, or collecting
    /// garbage the journal does not see.
    fn lifetime(&mut self, generation: u64, words: &[&str], line: usize) {
        let pid = words
            .iter()
            .find_map(|word| word.strip_prefix("pid=")?.parse::<u32>().ok());
        match words.first().copied() {
            Some("start") => {
                self.starts.push(line);
                self.lifetimes.insert(
                    generation,
                    Lifetime {
                        start: line,
                        pid,
                        shared: words.contains(&"shared"),
                        end: None,
                    },
                );
            }
            Some("end") => {
                self.clean_ends += 1;
                if let Some(lifetime) = self.lifetimes.get_mut(&generation) {
                    lifetime.end = Some(line);
                }
            }
            Some("dead") => {
                for lifetime in self.lifetimes.values_mut() {
                    if lifetime.shared && lifetime.end.is_none() && lifetime.pid == pid {
                        lifetime.end = Some(line);
                    }
                }
            }
            Some("gc-unseen") => {
                self.unseen_gc.insert(generation);
            }
            _ => {}
        }
    }

    /// The line by which `generation` had ended, if it has: its `g end`, the `g dead` of its
    /// process, or, for a generation that is not shared, the next generation's start.
    fn end_of(&self, generation: u64) -> Option<usize> {
        let lifetime = self.lifetimes.get(&generation)?;
        if lifetime.end.is_some() || lifetime.shared {
            return lifetime.end;
        }
        let next = self
            .starts
            .partition_point(|start| *start <= lifetime.start);
        self.starts.get(next).copied()
    }

    /// Whether `event` may still have been running after `line`: it ended after it, or never
    /// ended and its generation had not ended by then.
    fn running_after(&self, event: &Event, line: usize) -> bool {
        match event.end() {
            Some(end) => end > line,
            None => self.end_of(event.generation).is_none_or(|end| end > line),
        }
    }

    /// Whether `generation`, or a pass it started that ended at `end`, may have run after `line`.
    fn ran_after(&self, generation: u64, end: Option<usize>, line: usize) -> bool {
        end.or_else(|| self.end_of(generation))
            .is_none_or(|end| end > line)
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
        let covered = |event: &Event| covered(event, covered_before);
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
                .any(|event| self.running_after(event, last.line));
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
                    && (event.line > add.line || self.running_after(event, add.line))
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
                .any(|generation| self.ran_after(*generation, None, event.line))
                || self.passes.iter().any(|pass| {
                    self.ran_after(pass.generation, pass.end, event.line)
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

    /// What mutable key `key` must hold: the value its last store or swap left, once a flush of
    /// the mutable store that started after it succeeded, with nothing running alongside it.
    pub fn expect_value(&self, key: u64) -> Value {
        let Some(events) = self.mutable.get(&key) else {
            return Value::Absent;
        };
        // A swap that found another value changed nothing.
        let meaningful: Vec<&Event> = events.iter().filter(|event| !event.not_found).collect();
        let Some(last) = meaningful.last() else {
            return Value::Absent;
        };
        let overlapped = meaningful[..meaningful.len() - 1]
            .iter()
            .any(|event| self.running_after(event, last.line));
        if covered(last, &self.mutable_covered_before) && !overlapped {
            Value::Is(last.result())
        } else {
            Value::Unknown
        }
    }

    /// The swaps of mutable key `key` that succeeded, each with the value it swapped from and
    /// whether a flush settled it as [`History::expect_value`] settles a value.
    pub fn swaps(&self, key: u64) -> impl Iterator<Item = (u64, bool)> + '_ {
        self.mutable
            .get(&key)
            .into_iter()
            .flatten()
            .filter(|event| event.op == Op::Swap && event.ack.is_some())
            .map(|event| (event.value, covered(event, &self.mutable_covered_before)))
    }

    /// Whether a swap of mutable key `key` from `value` started that has not been shown to find
    /// another value.
    pub fn swap_started(&self, key: u64, value: u64) -> bool {
        self.mutable.get(&key).is_some_and(|events| {
            events
                .iter()
                .any(|event| event.op == Op::Swap && event.value == value && !event.not_found)
        })
    }

    /// Whether a store of `value` for mutable key `key` started.
    pub fn store_started(&self, key: u64, value: u64) -> bool {
        self.mutable.get(&key).is_some_and(|events| {
            events
                .iter()
                .any(|event| event.op == Op::MutableStore && event.value == value)
        })
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
            .all(|event| !self.running_after(event, stored.line))
    }
}

/// Whether `event` succeeded before a flush of its generation that started after it succeeded,
/// as `covered_before` records them.
fn covered(event: &Event, covered_before: &HashMap<u64, usize>) -> bool {
    event.ack.is_some_and(|ack| {
        covered_before
            .get(&event.generation)
            .is_some_and(|start| *start >= ack)
    })
}
