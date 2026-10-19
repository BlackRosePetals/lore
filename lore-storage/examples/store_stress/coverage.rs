// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! What a run exercised, so that a quiet run is not taken for coverage: which operations ran
//! alongside which, which were dropped unfinished in which file operation, what eviction and
//! compaction reclaimed, the events the store logged, and the shapes the store reached.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use lore_base::log::LoreLogLevel;
use lore_storage::LocalImmutableStore;
use lore_storage::gc_event::GcEventSink;
use lore_storage::test_util::StoreTask;
use parking_lot::Mutex;

static LOG_EVENTS: LazyLock<Mutex<BTreeMap<String, u64>>> = LazyLock::new(Mutex::default);

/// The store events a run should reach, by the words of the message the store logs for each,
/// numbers left out. Some are logged as the store sets out to do something, which their names
/// say. The last two take a machine failure or a damaged file, so a gap there is expected.
const MILESTONES: [(&str, &str); 15] = [
    ("compaction truncating a packfile", "truncating packfile"),
    (
        "compaction kept a referenced packfile",
        "keeping group packfile",
    ),
    (
        "compaction stopped by a request",
        "Packstore compactor stopping group before packfile",
    ),
    (
        "compaction skipped an unloaded group",
        "a bucket of it failed to load",
    ),
    (
        "compaction met an uncommitted fan-out",
        "its fan-out is not committed",
    ),
    (
        "compaction interrupted by a fan-out",
        "Packstore compactor stopping group at packfile the group fanned out",
    ),
    (
        "size eviction interrupted by a fan-out",
        "Size eviction for group stopping at bucket the group fanned out",
    ),
    ("size eviction evicted", "Size evicted for group"),
    ("capacity eviction evicted", "Evicted "),
    (
        "emptied packfile discarding",
        "Discard compacted and truncated packfile",
    ),
    (
        "obliterate found no other reference",
        "Fragment payload has no other references",
    ),
    (
        "obliterated fragment stored again",
        "Overwriting obliterated fragment",
    ),
    ("already obliterated", "already obliterated"),
    ("lost payload locations cleared", "Cleared "),
    ("corrupt bucket reset", "Resetting corrupt immutable bucket"),
];

fn log_callback(level: LoreLogLevel, _location: &str, message: &str) {
    if level < LoreLogLevel::Debug {
        return;
    }
    // Messages differ by the numbers and hashes in them; the words name the event. Enough of
    // them to hold every milestone's pattern whole.
    let template: String = message
        .split_whitespace()
        .filter(|word| !word.chars().any(|c| c.is_ascii_digit()))
        .take(16)
        .collect::<Vec<_>>()
        .join(" ");
    *LOG_EVENTS
        .lock()
        .entry(format!("{level:?} {template}"))
        .or_default() += 1;
}

/// Count every event the store logs at debug level and above from now on.
pub fn capture_logs() {
    lore_base::log::set_log_level(LoreLogLevel::Debug);
    lore_base::log::set_log_callback(Some(log_callback));
}

/// Counts what eviction and compaction report they did.
#[derive(Default)]
pub struct GcCounts {
    pub eviction_passes: AtomicU64,
    pub evicted: AtomicU64,
    pub compaction_passes: AtomicU64,
    pub compacted_bytes: AtomicU64,
}

impl GcEventSink for GcCounts {
    fn eviction_begin(&self, _target_fragments: u64) {
        self.eviction_passes.fetch_add(1, Ordering::Relaxed);
    }

    fn eviction_progress(&self, evicted: u64) {
        self.evicted.fetch_add(evicted, Ordering::Relaxed);
    }

    fn eviction_end(&self, _total_evicted: u64) {}

    fn compaction_begin(&self, _target_bytes: u64) {
        self.compaction_passes.fetch_add(1, Ordering::Relaxed);
    }

    fn compaction_progress(&self, compacted_bytes: u64) {
        self.compacted_bytes
            .fetch_add(compacted_bytes, Ordering::Relaxed);
    }

    fn compaction_end(&self, _total_compacted_bytes: u64) {}
}

/// What one eviction or compaction pass reports, passed on to the run's counts.
pub struct PassCounts {
    pub evicted: AtomicU64,
    run: Arc<GcCounts>,
}

impl PassCounts {
    pub fn new(run: Arc<GcCounts>) -> PassCounts {
        PassCounts {
            evicted: AtomicU64::new(0),
            run,
        }
    }
}

impl GcEventSink for PassCounts {
    fn eviction_begin(&self, target_fragments: u64) {
        self.run.eviction_begin(target_fragments);
    }

    fn eviction_progress(&self, evicted: u64) {
        self.evicted.fetch_add(evicted, Ordering::Relaxed);
        self.run.eviction_progress(evicted);
    }

    fn eviction_end(&self, total_evicted: u64) {
        self.run.eviction_end(total_evicted);
    }

    fn compaction_begin(&self, target_bytes: u64) {
        self.run.compaction_begin(target_bytes);
    }

    fn compaction_progress(&self, compacted_bytes: u64) {
        self.run.compaction_progress(compacted_bytes);
    }

    fn compaction_end(&self, total_compacted_bytes: u64) {
        self.run.compaction_end(total_compacted_bytes);
    }
}

/// Which operation kinds started while which others were running, and which were dropped
/// unfinished while which file operations were in flight.
pub struct Overlaps {
    names: Vec<&'static str>,
    /// Kept for the life of the process, which the fault shim reads them for, and where the shim
    /// keeps them, past it.
    running: &'static [AtomicUsize],
    counts: Vec<AtomicU64>,
    cancelled: Mutex<BTreeMap<(usize, String), u64>>,
}

impl Overlaps {
    /// Counts of the kinds `names` running, and after them of the tasks the store runs of its own
    /// accord, which the store counts itself, see [`StoreTask`].
    pub fn new(names: Vec<&'static str>) -> Overlaps {
        let names: Vec<&'static str> = names
            .into_iter()
            .chain(StoreTask::ALL.iter().map(|(_, name)| *name))
            .collect();
        let kinds = names.len();
        let running = crate::shim::counters(kinds)
            .unwrap_or_else(|| Vec::leak((0..kinds).map(|_| AtomicUsize::new(0)).collect()));
        lore_storage::test_util::count_store_tasks(&running[kinds - StoreTask::ALL.len()..]);
        Overlaps {
            names,
            running,
            counts: (0..kinds * kinds).map(|_| AtomicU64::new(0)).collect(),
            cancelled: Mutex::default(),
        }
    }

    /// Record an operation of kind `kind` dropped unfinished, by each file operation in flight in
    /// the process as it was: `no-file-op` for none, and `unattributed` without the fault shim to
    /// ask.
    pub fn cancelled(&self, kind: usize) {
        let phases = match crate::shim::in_flight() {
            Some(ops) if ops.is_empty() => vec!["no-file-op".to_string()],
            Some(ops) => ops,
            None => vec!["unattributed".to_string()],
        };
        let mut cancelled = self.cancelled.lock();
        for phase in phases {
            *cancelled.entry((kind, phase)).or_default() += 1;
        }
    }

    /// Print the operations dropped unfinished, by kind and file operation in flight.
    pub fn report_cancelled(&self) {
        for ((kind, phase), count) in self.cancelled.lock().iter() {
            println!("COVERAGE cancel {} {phase} {count}", self.names[*kind]);
        }
    }

    /// Tell the fault shim, when the process runs under it, which operations run.
    pub fn register_with_shim(&self) {
        crate::shim::register(self.running, &self.names);
    }

    /// Record an operation of kind `kind` starting, until the returned guard drops.
    pub fn start(&self, kind: usize) -> Running<'_> {
        let kinds = self.names.len();
        for other in 0..kinds {
            if self.running[other].load(Ordering::Relaxed) > 0 {
                self.counts[kind * kinds + other].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.running[kind].fetch_add(1, Ordering::Relaxed);
        Running {
            overlaps: self,
            kind,
        }
    }

    fn count(&self, kind: usize, other: usize) -> u64 {
        self.counts[kind * self.names.len() + other].load(Ordering::Relaxed)
    }

    fn index(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|known| *known == name)
    }

    /// Print the overlaps of every reading and writing kind with every collecting and flushing
    /// one, and return the pairs among `required` that never overlapped.
    pub fn report(&self, rows: &[&str], columns: &[&str]) -> Vec<String> {
        let mut missing = vec![];
        let mut lines = vec![];
        print!("  overlaps   ");
        for column in columns {
            print!("{column:>10}");
        }
        println!();
        for row in rows {
            let Some(kind) = self.index(row) else {
                continue;
            };
            print!("  {row:<10} ");
            for column in columns {
                let Some(other) = self.index(column) else {
                    continue;
                };
                let count = self.count(kind, other) + self.count(other, kind);
                print!("{count:>10}");
                lines.push(format!("COVERAGE overlap {row}+{column} {count}"));
                if count == 0 {
                    missing.push(format!("{row}+{column}"));
                }
            }
            println!();
        }
        for line in lines {
            println!("{line}");
        }
        missing
    }
}

pub struct Running<'a> {
    overlaps: &'a Overlaps,
    kind: usize,
}

impl Drop for Running<'_> {
    fn drop(&mut self) {
        self.overlaps.running[self.kind].fetch_sub(1, Ordering::Relaxed);
    }
}

/// The shapes a store reached, sampled while it runs: the highest fan-out level, the most
/// packfiles a group held, the most taking writes at once, and the packfiles settled.
#[derive(Default)]
pub struct Shapes {
    pub max_level: AtomicUsize,
    pub max_packfiles: AtomicUsize,
    pub max_writeable: AtomicUsize,
    pub settled: AtomicUsize,
    pub max_unsettled: AtomicUsize,
    pub samples: AtomicUsize,
}

impl Shapes {
    pub async fn sample(&self, store: &Arc<LocalImmutableStore>, groups: &[u8]) {
        for &group in groups {
            let group = group as usize;
            let level = store.group[group]
                .bucket_count
                .load(std::sync::atomic::Ordering::Relaxed);
            self.max_level.fetch_max(level, Ordering::Relaxed);
            let packstore = store.packstore(group);
            let Ok(sizes) = packstore.sizes().await else {
                continue;
            };
            self.max_packfiles.fetch_max(sizes.len(), Ordering::Relaxed);
            self.max_writeable
                .fetch_max(packstore.writeable_ids().await.len(), Ordering::Relaxed);
            // Ids are not looked up one by one: compaction may discard packfiles meanwhile.
            let mut unsettled = [lore_storage::PackfileSize::default(); 256];
            if let Some(count) = packstore.unsettled_sizes(&mut unsettled).await {
                self.settled
                    .fetch_max(sizes.len().saturating_sub(count), Ordering::Relaxed);
                self.max_unsettled.fetch_max(count, Ordering::Relaxed);
            }
        }
        self.samples.fetch_add(1, Ordering::Relaxed);
    }

    pub fn report(&self) {
        println!(
            "  shapes: highest fan-out level {}, most packfiles in a group {}, most taking writes {}, \
             most settled {}, most unsettled {} (over 13 forces a sync instead of recorded sizes), {} samples",
            self.max_level.load(Ordering::Relaxed),
            self.max_packfiles.load(Ordering::Relaxed),
            self.max_writeable.load(Ordering::Relaxed),
            self.settled.load(Ordering::Relaxed),
            self.max_unsettled.load(Ordering::Relaxed),
            self.samples.load(Ordering::Relaxed),
        );
    }
}

/// How many times the store logged `milestone`, one of [`MILESTONES`], since [`capture_logs`].
pub fn reached(milestone: &str) -> u64 {
    let Some((_, pattern)) = MILESTONES.iter().find(|(name, _)| *name == milestone) else {
        return 0;
    };
    LOG_EVENTS
        .lock()
        .iter()
        .filter(|(event, _)| event.contains(pattern.trim()))
        .map(|(_, count)| count)
        .sum()
}

/// Print the store's logged events and which milestones the run reached.
pub fn report_logs() -> Vec<String> {
    println!("  store events:");
    for (event, count) in LOG_EVENTS.lock().iter() {
        println!("    {count:>8} x {event}");
    }
    let mut missing = vec![];
    for (milestone, _) in MILESTONES {
        let reached = reached(milestone);
        println!("COVERAGE event {} {reached}", milestone.replace(' ', "-"));
        let needs_machine_failure = matches!(
            milestone,
            "lost payload locations cleared" | "corrupt bucket reset"
        );
        if reached == 0 && !needs_machine_failure {
            missing.push(milestone.to_string());
        }
    }
    missing
}
