// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! One of several processes sharing a store directory, running the workload of [`crate::work`] in
//! calls, as client processes use a store. A call claims both stores, runs a random number of
//! operations on a random number of tasks, flushes, and lets its claims go, and the process pauses
//! before the next. A process that keeps its stores (`--keeps`) reuses them from call to call,
//! reusing their state across holds of their locks while their epochs are unchanged, and now and
//! then ends a call with the stores unflushed, which keeps their flocks until a later call
//! flushes; one that does not opens them afresh for each call, its claims joining the ones they
//! were opened under.
//!
//! The processes append to one journal, see [`crate::journal`]. A call reads what the others
//! appended once it has claimed the stores, which no other process uses until it lets them go, so
//! what the journal settled holds for the whole call: the model the reads are held to is set from
//! it for every slot another generation touched, and the mutable reads are held to it, see
//! [`crate::mutable`]. A call also holds the loads its claims made to it. A kept store reloads at
//! its claim when another process wrote to it since its last call, which a write that succeeded
//! shows, and not when none could have. Stores opened afresh load once each.
//!
//! With `--gc-max-size` or `--gc-max-capacity`, a process collects the immutable store's garbage as
//! a client does: a load crossing a cap fires a pass, and with `--background-gc` the store's own
//! passes run every `--gc-delay-ms` between calls and during them. The journal does not see those
//! passes, so the generation is marked as collecting where it does not see, and another process
//! may have written the store whenever it was alive. With `--flush-background`, the immutable
//! store's durable writes schedule its delayed flush too, as a service's do, which then runs
//! between calls.
//!
//! With `--claimers <n>`, a call claims the stores from up to `n - 1` tasks besides its own claim,
//! all at once, as concurrent commands of a service do; each runs a few operations once its claim
//! is made. Every claim is journaled from when it is made until just before it goes, so that
//! `verify` checks that no two processes held a store at once. A store error is a violation unless
//! the shim fails file operations or the environment refuses them, see
//! [`crate::work::Work::errors_expected`], and so is a call that fails, unless it gave up on its
//! opening or its claim.

use std::collections::BTreeMap;
use std::ffi::c_long;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use lore_storage::ImmutableStore;
use lore_storage::LocalImmutableStore;
use lore_storage::local::immutable_store::ImmutableStoreCreateOptions;

use crate::args::Args;
use crate::client::Claims;
use crate::client::Loads;
use crate::client::Stores;
use crate::coverage;
use crate::journal::Added;
use crate::journal::History;
use crate::journal::Journal;
use crate::journal::Store;
use crate::model::Config;
use crate::model::Rng;
use crate::work::Mix;
use crate::work::Options;
use crate::work::Work;

/// The operations a call mixes in by default besides `work`'s: the mutable store's.
const MUTABLE_MIX: &str = "mstore=4,mload=4,mswap=6,mlist=0.5,mflush=0.5,mflushsync=0.2";

pub async fn main(args: &Args) -> u8 {
    let Some(dir) = args.dir() else {
        eprintln!("calls needs a store directory");
        return 2;
    };
    match run(&dir, args).await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("calls failed: {err}");
            3
        }
    }
}

async fn run(dir: &Path, args: &Args) -> Result<u8, String> {
    let config = Config::load_or_create(dir, args).map_err(|err| err.to_string())?;
    crate::work::prefill_near_limit(dir, &config, args);
    let journal = Arc::new(Journal::open(dir).map_err(|err| err.to_string())?);
    let rng = Rng::new(args.number("run-seed", journal.generation()) ^ config.seed);
    let flush_delay = args.number("flush-delay", 3600);
    let options = Options::from_args(args, flush_delay);
    let mut mix_spec = format!("{MUTABLE_MIX},{}", args.string("mix", ""));
    if config.real_hash && !mix_spec.contains("verify=") {
        mix_spec.push_str(",verify=0.05");
    }
    if !args.flag("quiet-store") {
        coverage::capture_logs();
        crate::work::report_repairs(&journal);
    }
    crate::work::lower_evictor_floor(args);
    let overlaps = coverage::Overlaps::new(crate::work::kind_names());
    overlaps.register_with_shim();
    let work = Work::new(
        config.clone(),
        journal.clone(),
        options,
        Mix::parse(Some(&mix_spec)),
        overlaps,
    );
    let hang_seconds = args.number("hang-seconds", 120);
    let watchdog = crate::work::watch_for_hangs(work.progress(), hang_seconds, journal.clone());
    let keeps = args.flag("keeps");
    let mut calls = Calls {
        dir: dir.to_path_buf(),
        config,
        keeps,
        collection: Collection::from_args(args),
        flush_delay,
        flush_background: args.flag("flush-background"),
        sync_flushes: args.float("sync-flushes", 0.0),
        claimers: args.number("claimers", 1).max(1),
        claimer_tasks: tokio::task::JoinSet::new(),
        call_ops: args.number("call-ops", 24).max(1),
        tasks: args.number("tasks", 4).max(1),
        unflushed: args.float("unflushed", 0.2),
        keep_alive: Duration::from_millis(args.number("keep-alive-ms", u64::MAX)),
        audit_files: args.float("audit-files", 0.0),
        faults: [
            crate::shim::faults_on("immutable"),
            crate::shim::faults_on("mutable"),
        ],
        loads: Loads::new(dir)?,
        history: History::default(),
        journal,
        work,
        rng,
        kept: None,
        last_call: Instant::now(),
        opened: false,
        loads_unaccounted: false,
        loads_at_end: [0; 2],
        coverage: BTreeMap::new(),
    };
    let pause_ms = args.number("pause-ms", 20);
    let deadline = Duration::from_secs_f64(args.float("seconds", 10.0));
    let started = Instant::now();
    while started.elapsed() < deadline {
        if let Err(err) = calls.call().await {
            calls.count("failed");
            eprintln!("call failed: {err}");
            calls.work.unexpected("call", &err);
        }
        let pause = calls.rng.below(pause_ms + 1);
        tokio::time::sleep(Duration::from_millis(pause)).await;
    }
    if calls.work.options().liveness
        && let Some(faults) = crate::shim::disarm()
        && let Err(err) = calls.probe(faults).await
    {
        calls.work.violation(
            "stuck-after-faults",
            0,
            0,
            &format!("after {faults} faults: {err}"),
        );
    }
    calls.end().await;
    watchdog.store(true, Ordering::Relaxed);
    calls.report(started.elapsed().as_secs_f64());
    Ok(u8::from(calls.work.violations() > 0))
}

/// The garbage collection a process runs on the immutable store it keeps, as a client does.
struct Collection {
    /// The caps a load crossing fires a pass at, 0 for none.
    max_size: usize,
    max_capacity: usize,
    /// How long the store's own passes wait between them, when they run.
    background: Option<Duration>,
}

impl Collection {
    /// The collection `args` ask for, if any.
    fn from_args(args: &Args) -> Option<Collection> {
        let collection = Collection {
            max_size: args.number("gc-max-size", 0) as usize,
            max_capacity: args.number("gc-max-capacity", 0) as usize,
            background: args
                .flag("background-gc")
                .then(|| Duration::from_millis(args.number("gc-delay-ms", 500))),
        };
        (collection.max_size > 0 || collection.max_capacity > 0).then_some(collection)
    }

    /// Set `store`'s caps, and start its own passes when they run.
    fn start(&self, store: &Arc<LocalImmutableStore>) {
        store.set_gc_caps(self.max_size, self.max_capacity, false);
        if let Some(delay) = self.background {
            let store: Arc<dyn ImmutableStore> = store.clone();
            lore_storage::maintenance::spawn_gc(
                &store,
                &ImmutableStoreCreateOptions {
                    max_capacity: (self.max_capacity > 0).then_some(self.max_capacity),
                    eviction_delay: Some(delay),
                    max_size: (self.max_size > 0).then_some(self.max_size),
                    compaction_delay: Some(delay),
                },
            );
        }
    }
}

/// How long the end of a collecting generation waits for its passes to let its stores go.
const COLLECTION_END_SECONDS: u64 = 100;

/// A process's calls.
struct Calls {
    dir: PathBuf,
    config: Config,
    keeps: bool,
    /// The collection a keeping process runs on its stores, if any.
    collection: Option<Collection>,
    flush_delay: u64,
    /// The most operations a call runs, and the most tasks it runs them on.
    call_ops: u64,
    tasks: u64,
    /// Whether the immutable store's durable writes schedule its delayed flush.
    flush_background: bool,
    /// The share of the flushes ending calls that sync what they write.
    sync_flushes: f64,
    /// The most claims a call makes at once, its own among them, and the tasks that made the
    /// others, which end within the call: they write the mutable store, whose reads the call holds
    /// to what it wrote.
    claimers: u64,
    claimer_tasks: tokio::task::JoinSet<()>,
    /// The share of a keeping process's calls that end without a flush of the stores.
    unflushed: f64,
    /// How long a keeping process keeps its stores after a call, as the client's keep-alive window
    /// does: past it, the next call flushes what they hold and opens them afresh.
    keep_alive: Duration,
    /// The share of flushed calls after which the bucket files on disk are held to the immutable
    /// store's memory, where neither faults nor the environment keep the audit from reading them.
    audit_files: f64,
    /// Whether the shim fails operations on the files of the immutable and of the mutable store,
    /// whose failed reads of the epoch reload a store with nothing written.
    faults: [bool; 2],
    loads: Loads,
    history: History,
    journal: Arc<Journal>,
    work: Arc<Work>,
    rng: Rng,
    /// The stores the last call kept, where the process keeps them.
    kept: Option<Stores>,
    /// When the last call ended.
    last_call: Instant,
    /// Whether a call opened stores yet, after which a process that opens them afresh starts a
    /// generation for each.
    opened: bool,
    /// Whether the last call failed before it read the journal, so that the loads of the next
    /// call's claims cannot be held to what the journal added since: a store the failed call
    /// claimed had loaded for writes the next call is the first to read.
    loads_unaccounted: bool,
    /// The loads when the last call ended. A kept store that a collection pass claimed between
    /// calls reloaded then for what another process wrote, not at the next call's claim.
    loads_at_end: [usize; 2],
    coverage: BTreeMap<&'static str, u64>,
}

impl Calls {
    fn count(&mut self, name: &'static str) {
        *self.coverage.entry(name).or_default() += 1;
    }

    /// Run one call.
    async fn call(&mut self) -> Result<(), String> {
        if self.last_call.elapsed() > self.keep_alive && self.kept.is_some() {
            self.count("keep-alive-expired");
            self.end().await;
        }
        let before = self.loads.now();
        let between = self.loads.since(self.loads_at_end);
        let (stores, openings) = self.stores(true).await?;
        let (claimed, go) = self.claim_all(&stores).await;
        let claims = match claimed {
            Ok(claims) => claims,
            Err(err) => {
                self.loads_unaccounted = true;
                drop((go, openings));
                self.join_claimers().await;
                self.end_call(stores).await;
                return Err(err);
            }
        };
        let opened = openings.is_some();
        drop(openings);
        let loads = self.loads.since(before);
        let since = self.history.lines;
        let added = match self.history.update(&self.dir) {
            Ok(added) => added,
            Err(err) => {
                self.loads_unaccounted = true;
                drop((go, claims));
                self.join_claimers().await;
                self.end_call(stores).await;
                return Err(format!("read the journal: {err}"));
            }
        };
        if std::mem::take(&mut self.loads_unaccounted) {
            self.count("loads-unaccounted");
        } else {
            self.check_loads(loads, between, opened, &added, since);
        }
        let generation = self.journal.generation();
        self.work.learn(
            &self.history,
            added
                .slots
                .iter()
                .filter(|(_, by)| *by != generation)
                .map(|(slot, _)| *slot),
        );
        self.work.begin_call(&self.history);
        go.send_replace(true);
        self.run_operations().await;
        if self.keeps && self.rng.chance(self.unflushed) {
            self.count("unflushed");
        } else if self.flush(&stores).await
            && !self.faults[0]
            && !self.work.options().environment_errors
            && self.rng.chance(self.audit_files)
        {
            self.count("files-audited");
            for finding in crate::verify::files_behind_memory(&stores.immutable).await {
                self.work
                    .violation("bucket-file-behind-memory", 0, 0, &finding);
            }
        }
        drop(claims);
        self.end_call(stores).await;
        Ok(())
    }

    /// The stores of a call: the kept ones, or ones opened for it, with the claims they were
    /// opened under. With `may_give_up`, the opening is given up on now and then, as a call's
    /// caller may give up on it.
    async fn stores(&mut self, may_give_up: bool) -> Result<(Stores, Option<Held>), String> {
        if let Some(stores) = self.kept.take() {
            return Ok((stores, None));
        }
        let work = self.work.clone();
        let opening = work.track(crate::work::OPEN);
        let give_up = self.give_up_after().filter(|_| may_give_up);
        let (stores, openings) = match give_up {
            Some(after) => {
                let Ok(opened) = tokio::time::timeout(after, self.open()).await else {
                    drop(opening);
                    self.count("open-cancelled");
                    self.work.cancelled(crate::work::OPEN);
                    return Err("cancelled opening the stores".to_string());
                };
                opened?
            }
            None => self.open().await?,
        };
        Ok((stores, Some(Held::new(openings, &self.journal))))
    }

    /// How long to wait before giving up on an opening or a claim, now and then, as on the
    /// operations, see [`crate::work::Options::cancel`].
    fn give_up_after(&mut self) -> Option<Duration> {
        let options = self.work.options();
        let (cancel, cancel_us) = (options.cancel, options.cancel_us);
        self.rng
            .chance(cancel / 2.0)
            .then(|| Duration::from_micros(self.rng.below(cancel_us + 1)))
    }

    /// Claim `stores` for a call, giving up after `give_up`, if given.
    async fn claim(&self, stores: &Stores, give_up: Option<Duration>) -> Result<Held, String> {
        let claiming = self.work.track(crate::work::CLAIM);
        let claimed = match give_up {
            Some(after) => tokio::time::timeout(after, stores.claim())
                .await
                .unwrap_or_else(|_| {
                    self.work.cancelled(crate::work::CLAIM);
                    Err("cancelled claiming the stores".to_string())
                }),
            None => stores.claim().await,
        };
        drop(claiming);
        claimed.map(|claims| Held::new(claims, &self.journal))
    }

    /// Claim `stores` for a call, and from up to `claimers - 1` tasks alongside, all at once, as
    /// concurrent commands of a service claim the stores it keeps: one claim loads what another
    /// process wrote, and the others wait for that load and join it rather than load again. Each
    /// task runs a few operations under its claim once the call has read what other processes
    /// journaled, which their reads are held to, as the returned sender says, then lets it go; a
    /// task whose call failed first runs none. Returns the call's claim once every claim is made or
    /// failed, so that the loads the call holds to the journal are all of theirs.
    async fn claim_all(
        &mut self,
        stores: &Stores,
    ) -> (Result<Held, String>, tokio::sync::watch::Sender<bool>) {
        let extra = self.rng.below(self.claimers);
        if extra > 0 {
            self.count("concurrent-claims");
        }
        let (go, learned) = tokio::sync::watch::channel(false);
        let (made, mut claimed) = tokio::sync::mpsc::unbounded_channel();
        for _ in 0..extra {
            let (stores, work, journal, made, mut learned) = (
                stores.clone(),
                self.work.clone(),
                self.journal.clone(),
                made.clone(),
                learned.clone(),
            );
            let (ops, seed) = (self.rng.below(4), self.rng.next());
            let tasks = &mut self.claimer_tasks;
            lore_base::lore_spawn!(tasks, async move {
                let claiming = work.track(crate::work::CLAIM);
                let claimed = stores.claim().await;
                drop(claiming);
                let _ = made.send(());
                drop(made);
                match claimed {
                    Ok(claims) => {
                        let held = Held::new(claims, &journal);
                        if learned.wait_for(|learned| *learned).await.is_ok() {
                            work.run_some(ops, seed).await;
                        }
                        drop(held);
                    }
                    Err(err) => work.unexpected("a concurrent claim", &err),
                }
            });
        }
        drop((made, learned));
        let give_up = self.give_up_after();
        let claim = self.claim(stores, give_up).await;
        while claimed.recv().await.is_some() {}
        (claim, go)
    }

    /// Wait for the tasks that claimed the stores alongside the call, see [`Self::claim_all`].
    async fn join_claimers(&mut self) {
        while let Some(joined) = self.claimer_tasks.join_next().await {
            if let Err(err) = joined
                && err.is_panic()
            {
                self.work.violation("panic", 0, 0, &err.to_string());
            }
        }
    }

    /// Check that the stores take writes again once the faults end, after `faults` of them, as
    /// [`Work::probe`] does, in a call of its own: on the kept stores, or on stores opened for it
    /// as a call opens them. An error is the stores refusing the call.
    async fn probe(&mut self, faults: c_long) -> Result<(), String> {
        let (stores, openings) = self.stores(false).await?;
        let claimed = self.claim(&stores, None).await;
        drop(openings);
        let result = match claimed {
            Ok(claims) => {
                self.work.probe(faults).await;
                drop(claims);
                Ok(())
            }
            Err(err) => Err(err),
        };
        self.end_call(stores).await;
        result
    }

    /// Open the stores for a call. Each store object's life is a generation, so stores opened
    /// after the process's first start one.
    async fn open(&mut self) -> Result<(Stores, Claims), String> {
        let (stores, openings) = Stores::open(
            &self.dir,
            &self.config,
            self.keeps,
            self.flush_delay,
            self.flush_background,
        )
        .await?;
        if self.opened {
            self.journal.renew().map_err(|err| err.to_string())?;
        }
        self.opened = true;
        self.journal.log(format_args!(
            "g start pid={} shared keeps={}",
            std::process::id(),
            u8::from(self.keeps)
        ));
        if let Some(collection) = &self.collection {
            self.journal.log(format_args!("g gc-unseen"));
            collection.start(&stores.immutable);
        }
        self.work
            .use_stores(Some(stores.immutable.clone()), Some(stores.mutable.clone()));
        Ok((stores, openings))
    }

    /// Keep the stores for the next call, or let them go and end their generation.
    async fn end_call(&mut self, stores: Stores) {
        self.count("calls");
        self.last_call = Instant::now();
        self.loads_at_end = self.loads.now();
        if self.keeps {
            self.kept = Some(stores);
            return;
        }
        self.end_generation(stores).await;
    }

    /// Let `stores` go and end their generation, once the tasks it spawned ended. A collecting
    /// generation ends once its passes, which hold the stores while they run, have let them go,
    /// so that none runs past its end.
    async fn end_generation(&mut self, stores: Stores) {
        self.work.join_spawned().await;
        self.work.use_stores(None, None);
        let immutable = Arc::downgrade(&stores.immutable);
        let mutable = Arc::downgrade(&stores.mutable);
        drop(stores);
        if self.collection.is_some() {
            let waited = Instant::now();
            while immutable.strong_count() + mutable.strong_count() > 0 {
                if waited.elapsed() > Duration::from_secs(COLLECTION_END_SECONDS) {
                    println!(
                        "NOTE store-outlived-its-generation: a collection pass held the stores \
                         {COLLECTION_END_SECONDS}s past their generation's end"
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        self.journal.log(format_args!("g end"));
    }

    /// Hold the loads a call's claims made to what the journal says another process did since
    /// this process's last call, the journal's line `since` on: see the module documentation.
    /// `between` are the loads since the last call ended, which a collection pass or a delayed
    /// flush that claimed a kept store made for another process's write before this call claimed
    /// it.
    fn check_loads(
        &mut self,
        loads: [usize; 2],
        between: [usize; 2],
        opened: bool,
        added: &Added,
        since: usize,
    ) {
        let generation = self.journal.generation();
        let collected = self.history.unseen_gc_after(since, generation);
        for (index, store) in [Store::Immutable, Store::Mutable].into_iter().enumerate() {
            let name = ["immutable", "mutable"][index];
            let loaded = loads[index];
            let reloaded_before = between[index] > 0;
            if opened {
                if loaded != 1 {
                    self.work.violation(
                        "opening-loads",
                        0,
                        0,
                        &format!("the {name} store loaded {loaded} times as a call opened it"),
                    );
                }
                continue;
            }
            let writes = added
                .writes
                .iter()
                .filter(|(of, by, _)| *of == store && *by != generation);
            let wrote = writes.clone().any(|(_, _, claimed)| *claimed);
            let may_have = writes.count() > 0 || (store == Store::Immutable && collected);
            self.count(match (loaded, wrote) {
                (0, _) => "kept-reused",
                (_, true) => "kept-reloaded-after-a-write",
                _ => "kept-reloaded",
            });
            let class = if loaded > 1 {
                "kept-store-reloaded-twice"
            } else if wrote && loaded == 0 && !reloaded_before {
                "kept-store-missed-a-write"
            } else if !may_have && loaded > 0 && !self.faults[index] {
                "kept-store-reloaded-for-nothing"
            } else {
                continue;
            };
            self.work.violation(
                class,
                0,
                0,
                &format!(
                    "the kept {name} store loaded {loaded} times at a claim, after {} writes by others",
                    added
                        .writes
                        .iter()
                        .filter(|(of, by, claimed)| *of == store && *by != generation && *claimed)
                        .count()
                ),
            );
        }
    }

    /// Run a random number of operations on a random number of tasks, now and then with a
    /// compaction and eviction task alongside, and wait for the tasks that claimed the stores
    /// alongside the call. Tasks spawned to outlive their operations, which write only the
    /// immutable store, end within the call, whose generation they journal under, or in a keeping
    /// process, whose generation is its life, at times run on past it, holding the claims they
    /// joined.
    async fn run_operations(&mut self) {
        self.work.allow(1 + self.rng.below(self.call_ops));
        let tasks = 1 + self.rng.below(self.tasks) as usize;
        let gc_tasks = usize::from(self.rng.chance(0.2));
        let mut set = tokio::task::JoinSet::new();
        self.work
            .spawn_workers(&mut set, self.rng.next(), tasks, gc_tasks);
        while let Some(joined) = set.join_next().await {
            if let Err(err) = joined
                && err.is_panic()
            {
                self.work.violation("panic", 0, 0, &err.to_string());
            }
        }
        self.join_claimers().await;
        if self.keeps && self.rng.chance(0.3) {
            self.count("spawned-past-the-call");
        } else {
            self.work.join_spawned().await;
        }
    }

    /// Flush the stores, as a call ends, now and then syncing, and now and then dropping a flush
    /// unfinished as the operations are, and return whether both flushed. A flush covers every
    /// operation of the call when no delayed flush can run alongside it.
    async fn flush(&mut self, stores: &Stores) -> bool {
        let strong = self.flush_delay >= 1000;
        let options = self.work.options();
        let mut flushed_both = true;
        for store in [Store::Immutable, Store::Mutable] {
            let cancel = self.rng.chance(options.cancel);
            let after = Duration::from_micros(self.rng.below(options.cancel_us + 1));
            let sync = self.rng.chance(self.sync_flushes);
            let ending = self.work.track(crate::work::END);
            let id = self.journal.flush_start(store, sync, strong);
            let flushed = match cancel {
                true => tokio::time::timeout(after, stores.flush_store(store, sync)).await,
                false => Ok(stores.flush_store(store, sync).await),
            };
            drop(ending);
            let flushed = flushed.map_or_else(
                |_| {
                    self.work.cancelled(crate::work::END);
                    Err("cancelled".to_string())
                },
                |flushed| flushed.map_err(|err| err.to_string()),
            );
            if let Err(err) = &flushed {
                eprintln!("a call's flush of the {store:?} store failed: {err}");
                self.work
                    .unexpected(&format!("a call's flush of the {store:?} store"), err);
            }
            self.journal.flush_end(store, id, flushed.is_ok());
            flushed_both &= flushed.is_ok();
        }
        flushed_both
    }

    /// Flush what a keeping process's last call left, and end its generation.
    async fn end(&mut self) {
        let Some(stores) = self.kept.take() else {
            return;
        };
        self.work.join_spawned().await;
        match self.claim(&stores, None).await {
            Ok(claims) => {
                self.flush(&stores).await;
                drop(claims);
            }
            Err(err) => self.work.unexpected("the last claim", &err),
        }
        self.end_generation(stores).await;
    }

    fn report(&self, elapsed: f64) {
        self.work.report(elapsed);
        for (name, count) in &self.coverage {
            println!("COVERAGE calls {name} {count}");
        }
        for (counter, increments) in self.work.increments() {
            println!("increments counter {counter}: {increments}");
        }
    }
}

/// Claims of the stores, journaled as made when they are, and as let go just before they go, see
/// [`crate::journal`].
struct Held {
    claims: Claims,
    journal: Arc<Journal>,
}

impl Held {
    fn new(claims: Claims, journal: &Arc<Journal>) -> Held {
        for (claim, store) in claims.iter().zip([Store::Immutable, Store::Mutable]) {
            if claim.is_some() {
                journal.claim(store, true);
            }
        }
        Held {
            claims,
            journal: journal.clone(),
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        for (claim, store) in self.claims.iter().zip([Store::Immutable, Store::Mutable]) {
            if claim.is_some() {
                self.journal.claim(store, false);
            }
        }
    }
}
