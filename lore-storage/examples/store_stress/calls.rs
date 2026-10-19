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

use std::collections::BTreeMap;
use std::ffi::c_long;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

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
const MUTABLE_MIX: &str = "mstore=4,mload=4,mswap=6,mlist=0.5";

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
    }
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
    let mut calls = Calls {
        dir: dir.to_path_buf(),
        config,
        keeps: args.flag("keeps"),
        flush_delay,
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
        coverage: BTreeMap::new(),
    };
    let pause_ms = args.number("pause-ms", 20);
    let deadline = Duration::from_secs_f64(args.float("seconds", 10.0));
    let started = Instant::now();
    while started.elapsed() < deadline {
        if let Err(err) = calls.call().await {
            calls.count("failed");
            eprintln!("call failed: {err}");
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

/// A process's calls.
struct Calls {
    dir: PathBuf,
    config: Config,
    keeps: bool,
    flush_delay: u64,
    /// The most operations a call runs, and the most tasks it runs them on.
    call_ops: u64,
    tasks: u64,
    /// The share of a keeping process's calls that end without a flush of the stores.
    unflushed: f64,
    /// How long a keeping process keeps its stores after a call, as the client's keep-alive window
    /// does: past it, the next call flushes what they hold and opens them afresh.
    keep_alive: Duration,
    /// The share of flushed calls after which the bucket files on disk are held to the immutable
    /// store's memory.
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
        let (stores, openings) = self.stores().await?;
        let claims = match self.claim(&stores).await {
            Ok(claims) => claims,
            Err(err) => {
                self.loads_unaccounted = true;
                self.end_call(stores);
                return Err(err);
            }
        };
        let opened = openings.is_some();
        drop(openings);
        let loads = self.loads.since(before);
        let added = match self.history.update(&self.dir) {
            Ok(added) => added,
            Err(err) => {
                self.loads_unaccounted = true;
                drop(claims);
                self.end_call(stores);
                return Err(format!("read the journal: {err}"));
            }
        };
        if std::mem::take(&mut self.loads_unaccounted) {
            self.count("loads-unaccounted");
        } else {
            self.check_loads(loads, opened, &added);
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
        self.run_operations().await;
        if self.keeps && self.rng.chance(self.unflushed) {
            self.count("unflushed");
        } else if self.flush(&stores).await && !self.faults[0] && self.rng.chance(self.audit_files)
        {
            self.count("files-audited");
            for finding in crate::verify::files_behind_memory(&stores.immutable).await {
                self.work
                    .violation("bucket-file-behind-memory", 0, 0, &finding);
            }
        }
        drop(claims);
        self.end_call(stores);
        Ok(())
    }

    /// The stores of a call: the kept ones, or ones opened for it, with the claims they were
    /// opened under.
    async fn stores(&mut self) -> Result<(Stores, Option<Claims>), String> {
        if let Some(stores) = self.kept.take() {
            return Ok((stores, None));
        }
        let work = self.work.clone();
        let _opening = work.track(crate::work::OPEN);
        let (stores, openings) = self.open().await?;
        Ok((stores, Some(openings)))
    }

    /// Claim `stores` for a call.
    async fn claim(&self, stores: &Stores) -> Result<Claims, String> {
        let _claiming = self.work.track(crate::work::CLAIM);
        stores.claim().await
    }

    /// Check that the stores take writes again once the faults end, after `faults` of them, as
    /// [`Work::probe`] does, in a call of its own: on the kept stores, or on stores opened for it
    /// as a call opens them. An error is the stores refusing the call.
    async fn probe(&mut self, faults: c_long) -> Result<(), String> {
        let (stores, openings) = self.stores().await?;
        let claimed = self.claim(&stores).await;
        drop(openings);
        let result = match claimed {
            Ok(claims) => {
                self.work.probe(faults).await;
                drop(claims);
                Ok(())
            }
            Err(err) => Err(err),
        };
        self.end_call(stores);
        result
    }

    /// Open the stores for a call. Each store object's life is a generation, so stores opened
    /// after the process's first start one.
    async fn open(&mut self) -> Result<(Stores, Claims), String> {
        let (stores, openings) =
            Stores::open(&self.dir, &self.config, self.keeps, self.flush_delay).await?;
        if self.opened {
            self.journal.renew().map_err(|err| err.to_string())?;
        }
        self.opened = true;
        self.journal.log(format_args!(
            "g start pid={} shared keeps={}",
            std::process::id(),
            u8::from(self.keeps)
        ));
        self.work
            .use_stores(Some(stores.immutable.clone()), Some(stores.mutable.clone()));
        Ok((stores, openings))
    }

    /// Keep the stores for the next call, or let them go and end their generation.
    fn end_call(&mut self, stores: Stores) {
        self.count("calls");
        self.last_call = Instant::now();
        if self.keeps {
            self.kept = Some(stores);
            return;
        }
        self.end_generation(stores);
    }

    /// Let `stores` go and end their generation.
    fn end_generation(&mut self, stores: Stores) {
        self.work.use_stores(None, None);
        drop(stores);
        self.journal.log(format_args!("g end"));
    }

    /// Hold the loads a call's claims made to what the journal says another process did since
    /// this process's last call: see the module documentation.
    fn check_loads(&mut self, loads: [usize; 2], opened: bool, added: &Added) {
        let generation = self.journal.generation();
        for (index, store) in [Store::Immutable, Store::Mutable].into_iter().enumerate() {
            let name = ["immutable", "mutable"][index];
            let loaded = loads[index];
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
            let may_have = writes.count() > 0;
            self.count(match (loaded, wrote) {
                (0, _) => "kept-reused",
                (_, true) => "kept-reloaded-after-a-write",
                _ => "kept-reloaded",
            });
            let class = if loaded > 1 {
                "kept-store-reloaded-twice"
            } else if wrote && loaded == 0 {
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
    /// compaction and eviction task alongside. Flushes spawned to outlive their operations end
    /// within the call, whose generation they journal under.
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
        self.work.join_spawned().await;
    }

    /// Flush the stores, as a call ends, now and then dropping a flush unfinished as the
    /// operations are, and return whether both flushed. A flush covers every operation of the call
    /// when no delayed flush can run alongside it.
    async fn flush(&mut self, stores: &Stores) -> bool {
        let strong = self.flush_delay >= 1000;
        let options = self.work.options();
        let mut flushed_both = true;
        for store in [Store::Immutable, Store::Mutable] {
            let cancel = self.rng.chance(options.cancel);
            let after = Duration::from_micros(self.rng.below(options.cancel_us + 1));
            let ending = self.work.track(crate::work::END);
            let id = self.journal.flush_start(store, false, strong);
            let flushed = match cancel {
                true => tokio::time::timeout(after, stores.flush_store(store)).await,
                false => Ok(stores.flush_store(store).await),
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
        if let Ok(claims) = stores.claim().await {
            self.flush(&stores).await;
            drop(claims);
        }
        self.end_generation(stores);
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
