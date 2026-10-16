// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The randomized workload: concurrent tasks storing, reading, copying and obliterating keys,
//! flushing, compacting and evicting, each operation journaled, and every payload read back
//! checked against its key.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use lore_storage::CallerBuffer;
use lore_storage::LocalImmutableStore;
use lore_storage::PayloadRead;
use lore_storage::StoreError;
use lore_storage::StoreMatch;
use lore_storage::StoreMatchResult;
use lore_storage::StoreObliterateStats;
use lore_storage::immutable_store::CopyBehavior;
use lore_storage::immutable_store::ImmutableStore;
use parking_lot::Mutex;

use crate::args::Args;
use crate::coverage;
use crate::journal::Journal;
use crate::model::Config;
use crate::model::Rng;
use crate::model::Served;
use crate::online;
use crate::online::Effect;
use crate::online::Outcome;
use crate::online::State;
use crate::shim;

/// Exit code of a run that saw a violation and stopped at once, keeping the store as it was.
pub const EXIT_VIOLATION: u8 = 99;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Put,
    PutForce,
    PutMeta,
    Get,
    GetInto,
    Metadata,
    Query,
    Copy,
    Obliterate,
    Flush,
    FlushSync,
    Compact,
    Evict,
    StopGc,
    LoadIndex,
    Verify,
    FailWrite,
    FailSync,
    PutInvalid,
}

impl Kind {
    const ALL: [(Kind, &'static str, f64); 19] = [
        (Kind::Put, "put", 30.0),
        (Kind::PutForce, "putf", 2.0),
        (Kind::PutMeta, "putm", 3.0),
        (Kind::Get, "get", 24.0),
        (Kind::GetInto, "getinto", 8.0),
        (Kind::Metadata, "meta", 4.0),
        (Kind::Query, "query", 4.0),
        (Kind::Copy, "copy", 6.0),
        (Kind::Obliterate, "ob", 3.0),
        (Kind::Flush, "flush", 2.0),
        (Kind::FlushSync, "flushsync", 0.5),
        (Kind::Compact, "compact", 0.6),
        (Kind::Evict, "evict", 0.6),
        (Kind::StopGc, "stopgc", 0.2),
        (Kind::LoadIndex, "loadindex", 0.1),
        (Kind::Verify, "verify", 0.0),
        (Kind::FailWrite, "failwrite", 0.0),
        (Kind::FailSync, "failsync", 0.0),
        (Kind::PutInvalid, "putbad", 0.0),
    ];

    fn name(self) -> &'static str {
        Kind::ALL
            .iter()
            .find(|(kind, _, _)| *kind == self)
            .map_or("?", |(_, name, _)| name)
    }

    /// Whether a caller may give up on an operation of this kind, dropping it unfinished.
    fn cancellable(self) -> bool {
        !matches!(
            self,
            Kind::StopGc | Kind::FailWrite | Kind::FailSync | Kind::PutInvalid
        )
    }
}

/// What the run does besides its operations, tracked with them: opening the store, ending the
/// run, and checking that the store took writes again once faults ended.
const ACTIVITIES: [&str; 3] = ["open", "end", "probe"];
const OPEN: usize = Kind::ALL.len();
const END: usize = Kind::ALL.len() + 1;
const PROBE: usize = Kind::ALL.len() + 2;

struct Mix {
    weights: Vec<(Kind, f64)>,
    total: f64,
}

impl Mix {
    fn parse(spec: Option<&str>) -> Mix {
        let mut weights: Vec<(Kind, f64)> = Kind::ALL
            .iter()
            .map(|(kind, _, weight)| (*kind, *weight))
            .collect();
        for item in spec.unwrap_or_default().split(',') {
            let Some((name, weight)) = item.split_once('=') else {
                continue;
            };
            // `*` sets every weight, so that a mix can name only the kinds it runs.
            if name == "*"
                && let Ok(weight) = weight.parse::<f64>()
            {
                for slot in &mut weights {
                    slot.1 = weight;
                }
                continue;
            }
            if let Some((kind, _, _)) = Kind::ALL.iter().find(|(_, known, _)| *known == name)
                && let Ok(weight) = weight.parse::<f64>()
                && let Some(slot) = weights.iter_mut().find(|(known, _)| known == kind)
            {
                slot.1 = weight;
            }
        }
        let total = weights.iter().map(|(_, weight)| weight).sum();
        Mix { weights, total }
    }

    fn pick(&self, rng: &mut Rng) -> Kind {
        let mut roll = (rng.next() >> 11) as f64 / (1u64 << 53) as f64 * self.total;
        for (kind, weight) in &self.weights {
            if roll < *weight {
                return *kind;
            }
            roll -= weight;
        }
        Kind::Get
    }
}

struct Options {
    seconds: f64,
    max_ops: u64,
    tasks: usize,
    hot_keys: u64,
    hot_share: f64,
    compact_max_size: Vec<usize>,
    evict_max_capacity: Vec<usize>,
    sync_gc: bool,
    end: String,
    stop_on_violation: bool,
    hang_seconds: u64,
    heal: bool,
    gc_tasks: usize,
    gc_pause_ms: u64,
    /// Explicit flushes and compactions one at a time, which with no delayed flush firing and no
    /// background collection makes each flush cover everything acknowledged before it.
    serialize_flushes: bool,
    strong_flushes: bool,
    /// The share of operations dropped unfinished, each after up to `cancel_us` microseconds.
    cancel: f64,
    cancel_us: u64,
    /// Whether to check, once the run ends, that the store recovered from the faults it met.
    liveness: bool,
}

/// Counters per operation kind: run, succeeded, and failures by message.
#[derive(Default)]
struct Stats {
    run: u64,
    ok: u64,
    errors: BTreeMap<String, u64>,
}

struct Work {
    config: Config,
    store: Arc<LocalImmutableStore>,
    journal: Arc<Journal>,
    options: Options,
    mix: Mix,
    stop: AtomicBool,
    started_ops: AtomicU64,
    progress: Arc<AtomicU64>,
    violations: AtomicUsize,
    stats: Mutex<BTreeMap<Kind, Stats>>,
    /// Keys an obliterate ran for in this run, which a read racing it may see zeroed.
    obliterating: Mutex<std::collections::HashSet<u64>>,
    overlaps: coverage::Overlaps,
    serial: tokio::sync::Mutex<()>,
    gc: Arc<coverage::GcCounts>,
    shapes: coverage::Shapes,
    /// Fragment lists stored, obliterated with their children, refused an obliterate for want of
    /// the list's payload, and refused one otherwise, as when the list or a child is not there.
    lists: [AtomicUsize; 4],
    model: online::Model,
}

pub async fn main(args: &Args) -> u8 {
    let Some(dir) = args.dir() else {
        eprintln!("work needs a store directory");
        return 2;
    };
    match run(&dir, args).await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("work failed: {err}");
            3
        }
    }
}

fn sizes(args: &Args, name: &str, default: &str) -> Vec<usize> {
    args.string(name, default)
        .split(',')
        .filter_map(crate::args::parse_number)
        .map(|size| size as usize)
        .collect()
}

#[expect(
    clippy::exit,
    reason = "a hung store and the exit end mode both leave without dropping the store"
)]
async fn run(dir: &Path, args: &Args) -> Result<u8, String> {
    let config = Config::load_or_create(dir, args).map_err(|err| err.to_string())?;
    let journal = Arc::new(Journal::open(dir).map_err(|err| err.to_string())?);
    let mut rng = Rng::new(args.number("run-seed", journal.generation) ^ config.seed);
    let flush_background = match args.get("flush-background") {
        Some(value) => value == "true",
        None => rng.chance(0.5),
    };
    let flush_delay = args.number("flush-delay", rng.below(3));
    let options = Options {
        seconds: args.float("seconds", 10.0),
        max_ops: args.number("ops", u64::MAX),
        tasks: args.number("tasks", 16).max(1) as usize,
        hot_keys: args.number("hot-keys", 48).max(1),
        hot_share: args.float("hot-share", 0.3),
        compact_max_size: sizes(args, "compact-max-size", "1,256k,4m,64m"),
        evict_max_capacity: sizes(args, "evict-max-capacity", "1,4096,1m"),
        sync_gc: args.flag("sync-gc"),
        end: args.string("end", "flush"),
        stop_on_violation: args.flag("stop-on-violation"),
        hang_seconds: args.number("hang-seconds", 120),
        heal: args.flag("heal"),
        gc_tasks: args.number("gc-tasks", 2) as usize,
        gc_pause_ms: args.number("gc-pause-ms", 5),
        serialize_flushes: args.flag("serialize-flushes"),
        strong_flushes: args.flag("serialize-flushes")
            && flush_delay >= 1000
            && !args.flag("background-gc"),
        cancel: args.float("cancel", 0.0),
        cancel_us: args.number("cancel-us", 2000),
        liveness: args.flag("liveness"),
    };
    let mut mix_spec = args.string("mix", "");
    if config.real_hash && !mix_spec.contains("verify=") {
        mix_spec.push_str(",verify=0.05");
    }
    let mix = Mix::parse(Some(&mix_spec));

    if let Some(near) = args
        .get("prefill-near-limit")
        .and_then(crate::args::parse_number)
    {
        prefill_near_limit(dir, &config, near);
    }

    if !args.flag("quiet-store") {
        coverage::capture_logs();
    }
    journal.log(format_args!(
        "g start pid={} flush_background={flush_background} flush_delay={flush_delay}",
        std::process::id()
    ));
    let overlaps = coverage::Overlaps::new(
        Kind::ALL
            .iter()
            .map(|(_, name, _)| *name)
            .chain(ACTIVITIES)
            .collect(),
    );
    overlaps.register_with_shim();
    let opening = overlaps.start(OPEN);
    let opened = Instant::now();
    let store = LocalImmutableStore::new(
        Some(dir.to_path_buf()),
        config.settings(flush_background, flush_delay),
    )
    .await
    .map_err(|err| format!("open: {err}"))?;
    println!(
        "opened generation {} in {:.3}s (flush_background={flush_background}, flush_delay={flush_delay})",
        journal.generation,
        opened.elapsed().as_secs_f64()
    );
    let dyn_store: Arc<dyn ImmutableStore> = store.clone();
    let gc_size = args.number("gc-max-size", 0) as usize;
    let gc_capacity = args.number("gc-max-capacity", 0) as usize;
    if gc_size > 0 || gc_capacity > 0 {
        // Collection the store starts itself reports to no sink of this run's.
        journal.log(format_args!("g gc-unseen"));
        store.set_gc_caps(gc_size, gc_capacity, false);
        if args.flag("background-gc") {
            lore_storage::maintenance::spawn_gc(
                &dyn_store,
                &lore_storage::local::immutable_store::ImmutableStoreCreateOptions {
                    max_capacity: (gc_capacity > 0).then_some(gc_capacity),
                    eviction_delay: Some(Duration::from_millis(args.number("gc-delay-ms", 500))),
                    max_size: (gc_size > 0).then_some(gc_size),
                    compaction_delay: Some(Duration::from_millis(args.number("gc-delay-ms", 500))),
                },
            );
        }
    }
    if args.flag("load-index") {
        let loaded = Instant::now();
        if let Err(err) = dyn_store.clone().load_index().await {
            eprintln!("load_index failed: {err}");
        }
        println!("loaded index in {:.3}s", loaded.elapsed().as_secs_f64());
    }
    drop(opening);

    let earlier = crate::journal::History::load(dir).map_err(|err| err.to_string())?;
    // The model starts from what the journal settled for the variants eviction may not take.
    let model = online::Model::new(!config.server && !options.heal);
    for &(key, variant) in earlier.events.keys() {
        if !config.evictable(variant) {
            let state = State::from(earlier.expect(key, variant, false));
            model.seed((key, variant), state, earlier.obliterated(key, variant));
        }
    }
    let progress = Arc::new(AtomicU64::new(0));
    let work = Arc::new(Work {
        config,
        store,
        journal: journal.clone(),
        options,
        mix,
        stop: AtomicBool::new(false),
        started_ops: AtomicU64::new(0),
        progress: progress.clone(),
        violations: AtomicUsize::new(0),
        stats: Mutex::new(BTreeMap::new()),
        obliterating: Mutex::new(earlier.obliterated_keys),
        overlaps,
        serial: tokio::sync::Mutex::new(()),
        gc: Arc::new(coverage::GcCounts::default()),
        shapes: coverage::Shapes::default(),
        lists: Default::default(),
        model,
    });

    let hang_seconds = work.options.hang_seconds;
    let watchdog_progress = progress.clone();
    let watchdog_stop = Arc::new(AtomicBool::new(false));
    let watchdog_done = watchdog_stop.clone();
    let watchdog_journal = journal.clone();
    std::thread::spawn(move || {
        // A second with a thread waiting on the storage, as on a sync it has not finished, is the
        // disk's, up to a limit: a hang is `hang_seconds` without progress the storage does not
        // explain, or a stall of any kind past the limit.
        const STORAGE_STALL_LIMIT: u64 = 600;
        let mut last = watchdog_progress.load(Ordering::Relaxed);
        let (mut still, mut on_storage) = (0, 0);
        while !watchdog_done.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_secs(1));
            let now = watchdog_progress.load(Ordering::Relaxed);
            if now == last {
                still += 1;
                on_storage += u64::from(waiting_on_storage());
                if still - on_storage >= hang_seconds || still >= STORAGE_STALL_LIMIT {
                    eprintln!(
                        "HANG: no operation finished for {still}s, {on_storage}s of it waiting \
                         on the storage"
                    );
                    watchdog_journal.log(format_args!("v hang 0 0 no-progress-{still}s"));
                    dump_threads();
                    std::process::exit(i32::from(EXIT_VIOLATION) + 1);
                }
            } else {
                if still >= hang_seconds {
                    eprintln!(
                        "STORAGE-STALL: no operation finished for {still}s, {on_storage}s of it \
                         waiting on the storage"
                    );
                }
                (still, on_storage) = (0, 0);
                last = now;
            }
        }
    });

    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..work.options.tasks {
        let work = work.clone();
        lore_base::lore_spawn!(tasks, async move { work.worker(index).await });
    }
    for index in 0..work.options.gc_tasks {
        let work = work.clone();
        lore_base::lore_spawn!(tasks, async move { work.gc_worker(index).await });
    }
    let sampler = {
        let work = work.clone();
        lore_base::lore_spawn!(async move {
            let groups: Vec<u8> = if work.config.groups.is_empty() {
                (0..=255).collect()
            } else {
                work.config.groups.clone()
            };
            let store: Arc<dyn ImmutableStore> = work.store.clone();
            while !work.stop.load(Ordering::Relaxed) {
                work.shapes.sample(&work.store, &groups).await;
                // The trait's remaining questions, which a service asks of a store now and then. A
                // store is unavailable while a bucket lock stays held past the timeout, which a
                // busy store may do; the watchdog catches one held for good.
                let _ = store.clone().fragment_count().await;
                if !store.clone().is_available(Duration::from_secs(5)).await {
                    println!("NOTE store-unavailable: a bucket lock was held past 5s");
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
    };
    let deadline = Duration::from_secs_f64(work.options.seconds);
    while !tasks.is_empty() {
        tokio::select! {
            joined = tasks.join_next() => {
                if let Some(Err(err)) = joined {
                    eprintln!("worker failed: {err}");
                    if err.is_panic() {
                        work.violation("panic", 0, 0, &format!("{err}"));
                    }
                }
            }
            () = tokio::time::sleep(Duration::from_millis(100)) => {
                if started.elapsed() >= deadline {
                    work.stop.store(true, Ordering::Relaxed);
                }
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    work.stop.store(true, Ordering::Relaxed);
    let _ = sampler.await;

    if work.options.liveness {
        work.probe().await;
    }
    let _ending = work.overlaps.start(END);
    let dyn_store: Arc<dyn ImmutableStore> = work.store.clone();
    if rng.chance(0.3) {
        // As a process shutting down does: collection stops for good, with passes in flight.
        dyn_store.clone().stop_gc(true).await;
    }
    match work.options.end.as_str() {
        "flush" | "sync" => {
            let sync = work.options.end == "sync";
            let id = journal.flush_start(sync, work.options.strong_flushes);
            let flushed = dyn_store.clone().flush(sync).await;
            if let Err(err) = &flushed {
                eprintln!("final flush failed: {err}");
            }
            journal.flush_end(id, flushed.is_ok());
            journal.log(format_args!("g end"));
        }
        "abort" => std::process::abort(),
        _ => {}
    }
    watchdog_stop.store(true, Ordering::Relaxed);
    work.report(elapsed);
    if work.violations.load(Ordering::Relaxed) > 0 {
        return Ok(1);
    }
    if work.options.end == "exit" {
        // Leave without dropping the store, as a process ending without a shutdown would.
        std::process::exit(0);
    }
    Ok(0)
}

/// The errors a file operation meets from the system rather than the store: EIO, ENOSPC, EDQUOT,
/// EMFILE, ENFILE, ENOMEM, EROFS and EMLINK.
const ENVIRONMENT_ERRORS: [i32; 8] = [
    libc::EIO,
    libc::ENOSPC,
    libc::EDQUOT,
    libc::EMFILE,
    libc::ENFILE,
    libc::ENOMEM,
    libc::EROFS,
    libc::EMLINK,
];

/// Whether what a put of an invalid fragment left is what a store may keep: nothing, the fragment
/// sanitized rather than as given, or an entry the put left alone. An entry's local flag says
/// whether it holds its payload, and short of a tombstone its flags and sizes are ones a put
/// accepts.
pub fn kept_sanitized(
    found: &lore_storage::local::immutable_store::ImmutableStoreFindResult,
) -> bool {
    use lore_storage::FragmentFlags as F;
    if found.matching != StoreMatch::MatchFull {
        return true;
    }
    let data = &found.data;
    let local = data.flags & F::PayloadStoredLocal.bits() != 0;
    let tombstone = data.flags & F::PayloadObliterated.bits() != 0;
    let fragment = lore_storage::Fragment {
        flags: data.flags,
        size_payload: data.size_payload,
        size_content: data.size_content,
    };
    local == (data.pack_file != 0)
        && (tombstone || lore_storage::validate_fragment_metadata(&fragment).is_ok())
}

/// A fragment no store should keep as given, its name, and the payload stored with it.
pub fn invalid_fragment(
    config: &Config,
    key: u64,
    variant: u32,
    rng: &mut Rng,
) -> (&'static str, lore_storage::Fragment, Option<bytes::Bytes>) {
    use lore_storage::FragmentFlags as F;
    let payload = config.payload(key);
    let len = payload.len();
    let mut fragment = config.fragment(key, variant);
    let base = fragment.flags;
    let name = match rng.below(10) {
        0 => {
            fragment.flags = F::PayloadStoredLocal.bits();
            return ("stored-local-without-payload", fragment, None);
        }
        1 => {
            fragment.flags = base | F::PayloadObliterated.bits();
            "obliterated-flag-from-caller"
        }
        2 => {
            fragment.flags = base | F::PayloadObliterating.bits();
            "obliterating-flag-from-caller"
        }
        3 => {
            fragment.flags = base | 1 << 30;
            "unknown-flag-bit"
        }
        4 => {
            fragment.flags =
                base | F::PayloadCompressedLZ4.bits() | F::PayloadCompressedZstd.bits();
            "two-compressors"
        }
        5 => {
            fragment.flags = base | F::PayloadCompressedLZ4.bits() | F::PayloadFragmented.bits();
            "compressed-and-fragmented"
        }
        6 => {
            fragment.size_content = len as u64 - 1;
            "size-payload-over-size-content"
        }
        7 => {
            fragment.size_content = len as u64 + 100;
            "plain-payload-shorter-than-content"
        }
        8 => {
            fragment.flags = base | F::PayloadFragmented.bits();
            "fragment-list-of-arbitrary-bytes"
        }
        _ => {
            fragment.size_content = (lore_storage::FRAGMENT_SIZE_THRESHOLD as u64) * 4;
            fragment.flags = F::PayloadStoredDurable.bits();
            return (
                "oversized-unfragmented-content-without-payload",
                fragment,
                None,
            );
        }
    };
    (name, fragment, Some(payload))
}

/// Whether a thread of this process waits in the kernel uninterruptibly, as on a sync or a read the
/// storage has not finished.
fn waiting_on_storage() -> bool {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return false;
    };
    tasks.flatten().any(|task| {
        std::fs::read_to_string(task.path().join("stat")).is_ok_and(|stat| {
            stat.rsplit_once(") ")
                .is_some_and(|(_, rest)| rest.starts_with('D'))
        })
    })
}

/// Print each thread's state and where it waits in the kernel, which is what a hang has to go on.
fn dump_threads() {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return;
    };
    for task in tasks.flatten() {
        let path = task.path();
        let name = std::fs::read_to_string(path.join("comm")).unwrap_or_default();
        let wchan = std::fs::read_to_string(path.join("wchan")).unwrap_or_default();
        let stat = std::fs::read_to_string(path.join("stat")).unwrap_or_default();
        let state = stat.split(' ').nth(2).unwrap_or("?");
        eprintln!(
            "thread {} {} state {state} wchan {wchan}",
            task.file_name().to_string_lossy(),
            name.trim()
        );
    }
}

/// Make packfile 1 of each group the keyspace uses a sparse file `near` bytes short of the size
/// limit, so that writes fill it and the store opens packfiles past it, unless the group already
/// has packfiles.
///
/// This runs under the shim's faults, so the file is only ever created new: a stat that fails
/// would take an existing packfile for absent, and truncating it would destroy every payload in
/// it. A group whose packfile cannot be created is left as it is.
fn prefill_near_limit(dir: &Path, config: &Config, near: u64) {
    const LIMIT: u64 = 3 * 1024 * 1024 * 1024;
    let groups: Vec<u8> = if config.groups.is_empty() {
        (0..=255).collect()
    } else {
        config.groups.clone()
    };
    for group in groups {
        let pack: PathBuf = dir
            .join("immutable")
            .join("index")
            .join(format!("{group:02x}"))
            .join("pack");
        if std::fs::create_dir_all(&pack).is_err() {
            continue;
        }
        let Ok(file) = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(pack.join("1"))
        else {
            continue;
        };
        let _ = file.set_len(LIMIT.saturating_sub(near));
    }
}

impl Work {
    fn pick_key(&self, rng: &mut Rng) -> u64 {
        if rng.chance(self.options.hot_share) {
            // A window of hot keys that moves with the run, so that the same keys meet each
            // operation at once while the window still covers the whole keyspace in time.
            let window = self.progress.load(Ordering::Relaxed) / 5000;
            (window * self.options.hot_keys + rng.below(self.options.hot_keys)) % self.config.keys
        } else {
            rng.below(self.config.keys)
        }
    }

    fn variant(&self, rng: &mut Rng) -> u32 {
        rng.below(u64::from(self.config.variants())) as u32
    }

    fn violation(&self, class: &str, key: u64, variant: u32, detail: &str) {
        self.violations.fetch_add(1, Ordering::Relaxed);
        let detail = detail.replace([' ', '\n'], "_");
        self.journal
            .log(format_args!("v {class} {key} {variant} {detail}"));
        eprintln!("VIOLATION {class} key {key} variant {variant}: {detail}");
        if self.options.stop_on_violation {
            std::process::exit(i32::from(EXIT_VIOLATION));
        }
    }

    fn record(&self, kind: Kind, result: Result<(), String>) {
        let mut stats = self.stats.lock();
        let stats = stats.entry(kind).or_default();
        stats.run += 1;
        match result {
            Ok(()) => stats.ok += 1,
            Err(message) => {
                // Group failures by the message without the parts naming a key or a path.
                let message: String = message
                    .split([':', '\n'])
                    .take(2)
                    .collect::<Vec<_>>()
                    .join(":")
                    .chars()
                    .filter(|c| !c.is_ascii_hexdigit() || c.is_alphabetic())
                    .take(120)
                    .collect();
                *stats.errors.entry(message).or_default() += 1;
            }
        }
    }

    fn report(&self, elapsed: f64) {
        let stats = self.stats.lock();
        let total: u64 = stats.values().map(|stats| stats.run).sum();
        println!(
            "generation {} ran {total} operations in {elapsed:.1}s ({:.0}/s), {} violations",
            self.journal.generation,
            total as f64 / elapsed.max(0.001),
            self.violations.load(Ordering::Relaxed)
        );
        for (kind, stats) in stats.iter() {
            println!(
                "  {:<10} {:>8} run {:>8} ok",
                kind.name(),
                stats.run,
                stats.ok
            );
            for (message, count) in &stats.errors {
                println!("      {count:>8} x {message}");
            }
        }
        drop(stats);
        self.report_coverage();
    }

    fn report_coverage(&self) {
        let gc = &self.gc;
        println!(
            "  gc: {} eviction passes evicted {} fragments, {} compaction passes reclaimed {} bytes",
            gc.eviction_passes.load(Ordering::Relaxed),
            gc.evicted.load(Ordering::Relaxed),
            gc.compaction_passes.load(Ordering::Relaxed),
            gc.compacted_bytes.load(Ordering::Relaxed),
        );
        self.shapes.report();
        let rows = [
            "put",
            "putf",
            "putm",
            "get",
            "getinto",
            "meta",
            "query",
            "copy",
            "ob",
            "compact",
            "evict",
            "flush",
            "loadindex",
        ];
        let columns = [
            "compact",
            "evict",
            "flush",
            "flushsync",
            "ob",
            "putf",
            "stopgc",
            "loadindex",
        ];
        let mut gaps = self.overlaps.report(&rows, &columns);
        gaps.extend(coverage::report_logs());
        println!(
            "COVERAGE gc {} {} {} {}",
            gc.eviction_passes.load(Ordering::Relaxed),
            gc.evicted.load(Ordering::Relaxed),
            gc.compaction_passes.load(Ordering::Relaxed),
            gc.compacted_bytes.load(Ordering::Relaxed),
        );
        println!(
            "COVERAGE lists {} {} {} {}",
            self.lists[0].load(Ordering::Relaxed),
            self.lists[1].load(Ordering::Relaxed),
            self.lists[2].load(Ordering::Relaxed),
            self.lists[3].load(Ordering::Relaxed),
        );
        println!(
            "COVERAGE shapes {} {} {} {} {}",
            self.shapes.max_level.load(Ordering::Relaxed),
            self.shapes.max_packfiles.load(Ordering::Relaxed),
            self.shapes.max_writeable.load(Ordering::Relaxed),
            self.shapes.settled.load(Ordering::Relaxed),
            self.shapes.max_unsettled.load(Ordering::Relaxed),
        );
        for gap in gaps {
            println!("COVERAGE-GAP {gap}");
        }
    }

    async fn worker(self: Arc<Self>, index: usize) {
        let mut rng = Rng::new(
            self.config.seed ^ (self.journal.generation << 32) ^ (index as u64).wrapping_mul(7919),
        );
        while !self.stop.load(Ordering::Relaxed)
            && self.started_ops.fetch_add(1, Ordering::Relaxed) < self.options.max_ops
        {
            let kind = self.mix.pick(&mut rng);
            let running = self.overlaps.start(kind as usize);
            let result = self.run_operation(kind, &mut rng).await;
            drop(running);
            self.record(kind, result);
            self.progress.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Compaction and eviction without pause, alongside the workers: each pass compacts toward a
    /// size, which evicts by size first, or evicts toward a capacity, and now and then a stop
    /// request cuts a pass short.
    async fn gc_worker(self: Arc<Self>, index: usize) {
        let mut rng = Rng::new(
            self.config.seed
                ^ (self.journal.generation << 32)
                ^ 0x6C00
                ^ (index as u64).wrapping_mul(104_729),
        );
        while !self.stop.load(Ordering::Relaxed)
            && self.started_ops.load(Ordering::Relaxed) < self.options.max_ops
        {
            let kind = match rng.below(10) {
                0..=5 => Kind::Compact,
                6..=8 => Kind::Evict,
                _ => Kind::StopGc,
            };
            let running = self.overlaps.start(kind as usize);
            let result = self.run_operation(kind, &mut rng).await;
            drop(running);
            self.record(kind, result);
            self.progress.fetch_add(1, Ordering::Relaxed);
            let pause = rng.below(self.options.gc_pause_ms + 1);
            if pause > 0 {
                tokio::time::sleep(Duration::from_millis(pause)).await;
            }
        }
    }

    fn dyn_store(&self) -> Arc<dyn ImmutableStore> {
        self.store.clone()
    }

    /// Whether the model follows `variant`, one eviction may not take.
    fn tracked(&self, variant: u32) -> bool {
        !self.config.evictable(variant)
    }

    /// Run one operation, now and then dropping it after a moment, as a caller giving up on it
    /// does.
    async fn run_operation(&self, kind: Kind, rng: &mut Rng) -> Result<(), String> {
        if self.options.cancel > 0.0 && kind.cancellable() && rng.chance(self.options.cancel) {
            let after = Duration::from_micros(rng.below(self.options.cancel_us + 1));
            return match tokio::time::timeout(after, self.operation(kind, rng)).await {
                Ok(result) => result,
                Err(_) => Err("cancelled".to_string()),
            };
        }
        self.operation(kind, rng).await
    }

    async fn operation(&self, kind: Kind, rng: &mut Rng) -> Result<(), String> {
        let key = self.pick_key(rng);
        let variant = self.variant(rng);
        let store = self.dyn_store();
        let config = &self.config;
        let partition = config.partition(variant);
        let address = config.address(key, variant);
        let slot = (key, variant);
        match kind {
            Kind::Put | Kind::PutForce | Kind::PutMeta => {
                // A list is stored as a client stores a file: its children first, each a put of
                // its own, then the list.
                if kind != Kind::PutMeta && config.is_list(key) {
                    for child in config.children(key) {
                        self.put(kind, child, variant).await?;
                    }
                }
                self.put(kind, key, variant).await?;
                if config.is_list(key) {
                    self.lists[0].fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }
            Kind::Get => {
                let settled = self.model.settled(slot);
                let result = store.get(partition, address).await;
                let settled = self.still(slot, settled);
                match result {
                    Ok(data) => {
                        match &data.payload {
                            Some(payload) => self.check(key, variant, payload, "get"),
                            None => self.violation("get-without-payload", key, variant, ""),
                        }
                        self.check_fragment(key, variant, data.fragment, "get");
                        let found = Found::Entry {
                            full: data.match_made == StoreMatch::MatchFull,
                            payload: data.payload.is_some(),
                        };
                        self.check_presence(settled, key, variant, found, "get");
                        Ok(())
                    }
                    Err(err) => {
                        self.check_presence(settled, key, variant, Found::of(&err), "get");
                        self.read_error(key, variant, err, "get")
                    }
                }
            }
            Kind::GetInto => {
                let len = config.payload_len(key);
                // Sometimes too small, to take the path refusing a destination without room.
                let capacity = match rng.below(10) {
                    0 => len.saturating_sub(1 + rng.below(16) as usize),
                    1..=3 => len + rng.below(4096) as usize,
                    _ => len,
                };
                let mut held = ReadBuffer {
                    bytes: vec![0xA5u8; capacity],
                    finished: false,
                };
                // SAFETY: `held` stays valid until the read completes, leaked if the read is
                // dropped unfinished, and nothing else touches it meanwhile.
                let mut destination =
                    unsafe { CallerBuffer::new(held.bytes.as_mut_ptr(), capacity) };
                let settled = self.model.settled(slot);
                let result = store.get_into(partition, address, &mut destination).await;
                held.finished = true;
                let buffer = std::mem::take(&mut held.bytes);
                let settled = self.still(slot, settled);
                let found = Found::Entry {
                    full: true,
                    payload: true,
                };
                match result {
                    Ok((fragment, PayloadRead::IntoBuffer)) => {
                        let read = fragment.size_payload as usize;
                        if read > capacity {
                            self.violation("get-into-overflow", key, variant, "");
                        } else {
                            self.check(key, variant, &buffer[..read], "get_into");
                            if buffer[read..].iter().any(|byte| *byte != 0xA5) {
                                self.violation("get-into-wrote-past-payload", key, variant, "");
                            }
                        }
                        self.check_presence(settled, key, variant, found, "get_into");
                        Ok(())
                    }
                    Ok((_, PayloadRead::Returned(payload))) => {
                        self.check(key, variant, &payload, "get_into-returned");
                        self.check_presence(settled, key, variant, found, "get_into");
                        Ok(())
                    }
                    Err(err) => {
                        if capacity < len && matches!(err, StoreError::Oversized(_)) {
                            return Ok(());
                        }
                        self.check_presence(settled, key, variant, Found::of(&err), "get_into");
                        self.read_error(key, variant, err, "get_into")
                    }
                }
            }
            Kind::Metadata => {
                let settled = self.model.settled(slot);
                let result = store.get_metadata(partition, address).await;
                let settled = self.still(slot, settled);
                match result {
                    Ok(data) => {
                        if data.match_made != StoreMatch::MatchNone {
                            self.check_fragment(key, variant, data.fragment, "get_metadata");
                        }
                        let local = data.fragment.flags
                            & lore_storage::FragmentFlags::PayloadStoredLocal.bits()
                            != 0;
                        let found = match data.match_made {
                            StoreMatch::MatchNone => Found::NoEntry,
                            matched => Found::Entry {
                                full: matched == StoreMatch::MatchFull,
                                payload: local,
                            },
                        };
                        self.check_presence(settled, key, variant, found, "get_metadata");
                        Ok(())
                    }
                    Err(err) => self.read_error(key, variant, err, "get_metadata"),
                }
            }
            Kind::Query => {
                // Every address under the partition asked about, each under some context.
                let count = 1 + rng.below(16) as usize;
                let first = variant / config.contexts * config.contexts;
                let slots: Vec<(u64, u32)> = (0..count)
                    .map(|_| {
                        let key = self.pick_key(rng);
                        (key, first + rng.below(u64::from(config.contexts)) as u32)
                    })
                    .collect();
                let addresses: Vec<_> = slots
                    .iter()
                    .map(|(key, variant)| config.address(*key, *variant))
                    .collect();
                let settled: Vec<_> = slots.iter().map(|slot| self.model.settled(*slot)).collect();
                let mut results = vec![StoreMatchResult::default(); count];
                let result = store.query(partition, &addresses, &mut results).await;
                if let Err(err) = result {
                    return self.read_error(key, variant, err, "query");
                }
                for ((slot, settled), result) in slots.iter().zip(settled).zip(&results) {
                    let found = match result.match_made {
                        StoreMatch::MatchNone => Found::NoEntry,
                        matched => Found::Entry {
                            full: matched == StoreMatch::MatchFull,
                            payload: result.stored_local,
                        },
                    };
                    let settled = self.still(*slot, settled);
                    self.check_presence(settled, slot.0, slot.1, found, "query");
                }
                Ok(())
            }
            Kind::Copy => {
                let destination = self.variant(rng);
                let mut source_address = address;
                if rng.chance(0.3) {
                    // A zero context names any association the source partition holds.
                    source_address.context = lore_storage::Context::from([0u8; 16]);
                }
                let running =
                    self.model
                        .begin((key, destination), Effect::Copy, self.tracked(destination));
                let id = self.journal.start("cp", key, destination);
                let result = store
                    .copy(
                        partition,
                        source_address,
                        config.partition(destination),
                        config.context(destination),
                        CopyBehavior {
                            durable: config.evictable(destination) && !config.server,
                            do_not_replicate: false,
                        },
                    )
                    .await;
                match result {
                    Ok(()) => {
                        self.journal.ack(id);
                        running.finish(Outcome::Done);
                        Ok(())
                    }
                    Err(err) => {
                        // A copy without a source found nothing to change.
                        let not_found = matches!(err, StoreError::AddressNotFound(_));
                        self.journal.fail(id, not_found);
                        running.finish(match not_found {
                            true => Outcome::NotFound,
                            false => Outcome::Failed,
                        });
                        Err(err.to_string())
                    }
                }
            }
            Kind::Obliterate => {
                // Obliterating a live list obliterates the children it references under the same
                // partition and context, so each is journaled with it; one already tombstoned
                // leaves them, which the history works out from the list's own.
                let children = match config.is_list(key) {
                    true => config.children(key),
                    false => Vec::new(),
                };
                {
                    let mut obliterating = self.obliterating.lock();
                    obliterating.insert(key);
                    obliterating.extend(children.iter().copied());
                }
                let tracked = self.tracked(variant);
                let mut running = vec![self.model.begin(slot, Effect::Obliterate, tracked)];
                running.extend(children.iter().map(|child| {
                    self.model
                        .begin((*child, variant), Effect::ObliterateChild, tracked)
                }));
                let mut ids = vec![self.journal.start("ob", key, variant)];
                ids.extend(
                    children
                        .iter()
                        .map(|child| self.journal.start_child(*child, variant, key)),
                );
                let result = store
                    .obliterate(
                        partition,
                        address,
                        Arc::new(StoreObliterateStats::default()),
                    )
                    .await;
                let list = usize::from(config.is_list(key));
                match result {
                    Ok(()) => {
                        for id in ids {
                            self.journal.ack(id);
                        }
                        for running in running {
                            running.finish(Outcome::Done);
                        }
                        self.lists[1].fetch_add(list, Ordering::Relaxed);
                        Ok(())
                    }
                    Err(err) => {
                        for id in ids {
                            self.journal.fail(id, false);
                        }
                        for running in running {
                            running.finish(Outcome::Failed);
                        }
                        let refused = match err {
                            StoreError::PayloadNotFound(_) => 2,
                            _ => 3,
                        };
                        self.lists[refused].fetch_add(list, Ordering::Relaxed);
                        Err(err.to_string())
                    }
                }
            }
            Kind::Flush | Kind::FlushSync => {
                let sync = kind == Kind::FlushSync;
                let _serial = match self.options.serialize_flushes {
                    true => Some(self.serial.lock().await),
                    false => None,
                };
                let id = self.journal.flush_start(sync, self.options.strong_flushes);
                let result = store.flush(sync).await;
                self.journal.flush_end(id, result.is_ok());
                result.map_err(|err| err.to_string())
            }
            Kind::Compact => {
                let _serial = match self.options.serialize_flushes {
                    true => Some(self.serial.lock().await),
                    false => None,
                };
                let sizes = &self.options.compact_max_size;
                let max_size = sizes[rng.below(sizes.len() as u64) as usize];
                let counts = Arc::new(coverage::PassCounts::new(self.gc.clone()));
                let id = self.journal.gc_start(true);
                let mut at = store.clone().compact_resume_at().await;
                let result = loop {
                    let sync = self.options.sync_gc || rng.chance(0.25);
                    match store
                        .clone()
                        .compact(
                            max_size,
                            at,
                            sync,
                            Some(counts.clone() as lore_storage::gc_event::GcEventSinkRef),
                        )
                        .await
                    {
                        Ok(Some(next)) => at = Some(next),
                        Ok(None) => break Ok(()),
                        Err(err) => break Err(err.to_string()),
                    }
                };
                self.journal.gc_end(id, true, 0);
                result
            }
            Kind::Evict => {
                let capacities = &self.options.evict_max_capacity;
                let capacity = capacities[rng.below(capacities.len() as u64) as usize];
                let counts = Arc::new(coverage::PassCounts::new(self.gc.clone()));
                let id = self.journal.gc_start(false);
                let result = store
                    .evict(
                        capacity,
                        self.options.sync_gc,
                        Some(counts.clone() as lore_storage::gc_event::GcEventSinkRef),
                    )
                    .await;
                // A failed pass may have removed entries before it failed.
                let removed = match &result {
                    Ok(evicted) => *evicted as u64,
                    Err(_) => u64::MAX,
                };
                self.journal.gc_end(id, false, removed);
                result.map(drop).map_err(|err| err.to_string())
            }
            Kind::StopGc => {
                store.stop_gc(false).await;
                Ok(())
            }
            Kind::LoadIndex => store.load_index().await.map_err(|err| err.to_string()),
            Kind::Verify => {
                let heal = self.options.heal && rng.chance(0.2);
                if heal {
                    self.journal.start("heal", 0, 0);
                }
                store.verify(heal).await.map_err(|err| err.to_string())
            }
            Kind::PutInvalid => {
                let (name, fragment, payload) = invalid_fragment(config, key, variant, rng);
                let result = store
                    .put(partition, address, fragment, payload, false)
                    .await;
                if result.is_ok()
                    && let Ok(found) = self.store.find(partition, address).await
                    && !kept_sanitized(&found)
                {
                    self.violation(
                        "put-accepted-invalid",
                        key,
                        variant,
                        &format!(
                            "{name}: stored flags {:#x} sizes {}/{} packfile {}",
                            found.data.flags,
                            found.data.size_payload,
                            found.data.size_content,
                            found.data.pack_file
                        ),
                    );
                }
                result.map_err(|err| err.to_string())
            }
            Kind::FailWrite => {
                lore_storage::test_util::fail_packfile_writes(1);
                Ok(())
            }
            Kind::FailSync => {
                lore_storage::test_util::fail_packfile_syncs(1);
                Ok(())
            }
        }
    }

    /// What the model held for `slot` before a read, if no operation on it ran alongside the read.
    fn still(&self, slot: (u64, u32), settled: Option<(State, u64)>) -> Option<State> {
        let (state, version) = settled?;
        self.model.still(slot, version).then_some(state)
    }

    /// Hold what a read found to what the run settled for its key and variant.
    fn check_presence(
        &self,
        settled: Option<State>,
        key: u64,
        variant: u32,
        found: Found,
        via: &str,
    ) {
        let Some(state) = settled else {
            return;
        };
        let class = match (state, found) {
            (State::Present { .. }, Found::NoEntry | Found::Entry { full: false, .. }) => {
                "lost-entry"
            }
            (State::Present { payload: true }, Found::NoPayload) => "lost-payload",
            // Metadata and queries say whether the payload is held, which a read would serve.
            (State::Present { payload: true }, Found::Entry { payload: false, .. }) => {
                "local-flag-missing"
            }
            (State::Obliterated, Found::Entry { full: true, .. }) => "obliteration-undone",
            _ => return,
        };
        self.violation(
            class,
            key,
            variant,
            &format!("{via} found {found:?} where the run settled {state:?}"),
        );
    }

    fn check(&self, key: u64, variant: u32, bytes: &[u8], via: &str) {
        match self.config.classify(key, bytes) {
            Served::Correct => {}
            served @ (Served::Zeros | Served::PartlyZeroed)
                if self.obliterating.lock().contains(&key) =>
            {
                self.violation(
                    "zeros-after-obliterate",
                    key,
                    variant,
                    &format!("{via} served {served} ({} bytes)", bytes.len()),
                );
            }
            served => self.violation(
                "corrupt-read",
                key,
                variant,
                &format!("{via} served {served} ({} bytes)", bytes.len()),
            ),
        }
    }

    fn check_fragment(&self, key: u64, variant: u32, fragment: lore_storage::Fragment, via: &str) {
        let len = self.config.payload_len(key) as u64;
        let content = self.config.content_len(key);
        if fragment.size_content != content
            || (fragment.size_payload != 0 && u64::from(fragment.size_payload) != len)
        {
            self.violation(
                "wrong-fragment",
                key,
                variant,
                &format!(
                    "{via} described size_payload {} size_content {} for a {len} byte payload of \
                     {content} bytes of content",
                    fragment.size_payload, fragment.size_content
                ),
            );
        }
    }

    /// Store `key` under `variant` the way `kind` does, journaled.
    async fn put(&self, kind: Kind, key: u64, variant: u32) -> Result<(), String> {
        let config = &self.config;
        let (op, force, payload) = match kind {
            Kind::Put => ("put", false, Some(config.payload(key))),
            Kind::PutForce => ("putf", true, Some(config.payload(key))),
            _ => ("putm", false, None),
        };
        let effect = match payload {
            Some(_) => Effect::Store,
            None => Effect::Describe,
        };
        let running = self
            .model
            .begin((key, variant), effect, self.tracked(variant));
        let id = self.journal.start(op, key, variant);
        let mut fragment = config.fragment(key, variant);
        if payload.is_none() {
            // A representation without its bytes: not local, and durable only where the variant
            // is, so that eviction may take it only where it may take the rest.
            fragment.flags &= !lore_storage::FragmentFlags::PayloadStoredLocal.bits();
        }
        let result = self
            .dyn_store()
            .put(
                config.partition(variant),
                config.address(key, variant),
                fragment,
                payload,
                force,
            )
            .await;
        match result {
            Ok(()) => {
                self.journal.ack(id);
                running.finish(Outcome::Done);
                Ok(())
            }
            Err(err) => {
                self.journal.fail(id, false);
                running.finish(Outcome::Failed);
                let message = err.to_string();
                if message.contains("collision") {
                    self.violation("collision", key, variant, &message);
                }
                // A write read back as other bytes, with no fault injected and the system refusing
                // nothing, is a write the store got wrong.
                if message.contains("Failed to verify written data")
                    && std::env::var_os("LD_PRELOAD").is_none()
                    && os_error(&err).is_none_or(|code| !ENVIRONMENT_ERRORS.contains(&code))
                {
                    self.violation("write-verify-failed", key, variant, &message);
                }
                Err(message)
            }
        }
    }

    fn read_error(&self, key: u64, variant: u32, err: StoreError, via: &str) -> Result<(), String> {
        match err {
            StoreError::AddressNotFound(_) | StoreError::PayloadNotFound(_) => Ok(()),
            err => {
                let message = err.to_string();
                // A read that fails points where the store holds nothing it can serve, unless the
                // system refused the read: no descriptor or memory left, no space, no quota, or a
                // failing device. Those reads fail as they should, and are counted apart. An
                // interrupted read is one the store could have retried.
                let refused = match os_error(&err) {
                    Some(code) if ENVIRONMENT_ERRORS.contains(&code) => "refused by the system",
                    Some(libc::EINTR) => {
                        println!("REPORT interrupted-read-surfaced: {via}: {message}");
                        "interrupted"
                    }
                    _ => {
                        self.violation(
                            "unreadable-entry",
                            key,
                            variant,
                            &format!("{via}: {message}"),
                        );
                        return Err(message);
                    }
                };
                *self
                    .stats
                    .lock()
                    .entry(Kind::Get)
                    .or_default()
                    .errors
                    .entry(refused.to_string())
                    .or_default() += 1;
                Err(message)
            }
        }
    }

    /// Once the faults end, the store must take puts and flushes again in the same process: one
    /// that refuses them until it is reopened was left broken by a transient error. A failed sync
    /// may keep durable flushes failing, which the store documents, so the check flushes without.
    async fn probe(&self) {
        let Some(faults) = shim::disarm() else {
            return;
        };
        let _probing = self.overlaps.start(PROBE);
        let config = &self.config;
        // Two plain keys of each group the keyspace uses.
        let mut per_group = [0u8; 256];
        let keys: Vec<u64> = (0..config.keys)
            .filter(|key| {
                let group = config.group_of(*key) as usize;
                let take = !config.is_list(*key) && per_group[group] < 2;
                per_group[group] += u8::from(take);
                take
            })
            .collect();
        let mut failures = vec![];
        for key in &keys {
            if let Err(message) = self.put(Kind::PutForce, *key, 0).await {
                failures.push(format!("put of key {key}: {message}"));
            }
        }
        let id = self.journal.flush_start(false, self.options.strong_flushes);
        let flushed = self.dyn_store().flush(false).await;
        self.journal.flush_end(id, flushed.is_ok());
        match flushed {
            // A packfile whose sync failed never settles, and with more unsettled packfiles than a
            // bucket file records the sizes of, a plain flush syncs them instead, which fails for
            // good until the process restarts. Reported, as the store chooses it.
            Err(err) if err.to_string().contains("failed an earlier sync") => {
                println!("REPORT plain-flush-blocked-by-a-failed-sync: {err}");
            }
            Err(err) => failures.push(format!("flush: {err}")),
            Ok(()) => {}
        }
        if !config.evictable(0) {
            for key in &keys {
                let address = config.address(*key, 0);
                match self.dyn_store().get(config.partition(0), address).await {
                    Ok(data) => match data.payload {
                        Some(payload) => self.check(*key, 0, &payload, "probe"),
                        None => failures.push(format!("get of key {key} served no payload")),
                    },
                    Err(err) => failures.push(format!("get of key {key}: {err}")),
                }
            }
        }
        println!(
            "probe after {faults} faults: {} keys stored, {} failures",
            keys.len(),
            failures.len()
        );
        if !failures.is_empty() {
            self.violation(
                "stuck-after-faults",
                0,
                0,
                &format!("after {faults} faults: {}", failures.join("; ")),
            );
        }
    }
}

/// The destination of a read into a caller's buffer, leaked when the read is dropped unfinished:
/// the store's read may still land after its future is gone, and `CallerBuffer::new` requires the
/// memory to stay valid until it does.
struct ReadBuffer {
    bytes: Vec<u8>,
    finished: bool,
}

impl Drop for ReadBuffer {
    fn drop(&mut self) {
        if !self.finished {
            std::mem::take(&mut self.bytes).leak();
        }
    }
}

/// What a read found, for [`Work::check_presence`].
#[derive(Clone, Copy, Debug)]
enum Found {
    /// The entry, as a full match when `full`, its payload served or said to be held when
    /// `payload`.
    Entry {
        full: bool,
        payload: bool,
    },
    NoEntry,
    NoPayload,
    /// An error that says nothing of what the store holds.
    Other,
}

impl Found {
    fn of(err: &StoreError) -> Found {
        match err {
            StoreError::AddressNotFound(_) => Found::NoEntry,
            StoreError::PayloadNotFound(_) => Found::NoPayload,
            _ => Found::Other,
        }
    }
}

/// The OS error behind `err`: the code an I/O error among its sources holds, or the one its
/// message names.
fn os_error(err: &StoreError) -> Option<i32> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(error) = source {
        if let Some(code) = error
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::raw_os_error)
        {
            return Some(code);
        }
        source = error.source();
    }
    let message = err.to_string();
    let start = message.rfind("(os error ")? + "(os error ".len();
    message[start..].split(')').next()?.parse().ok()
}
