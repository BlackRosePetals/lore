# Local store stress suite

A stress and crash suite for the local immutable store (`lore-storage/src/local/immutable_store.rs`
and `lore-storage/src/packstore.rs`), and for the local stores shared by processes
(`lore-storage/src/local/store_lock.rs`, with `lore-storage/src/local/mutable_store.rs`), run by
hand when the stores change, not in CI. It runs on Linux.

It looks for four things:

- **Corruption.** Every payload read back, and every payload location read straight from the
  packfiles, must be the bytes stored for that hash.
- **Loss.** An entry a flush acknowledged must survive the process being killed afterwards.
- **Broken guarantees.** An obliterated association must stay obliterated, `put` must refuse
  invalid fragments, and an entry's flags must agree with its data.
- **Stale state.** A process that keeps its stores between calls must reload a store another
  process wrote to since, and only then, and no update another process acknowledged may be lost,
  in either store.

It builds every extreme that arises on its own, short of a machine failure. Nothing outside the
store writes over or removes its files. A power loss or kernel crash is out of scope for the runs;
a durability gap only such a failure would expose is reported, not tested for.

- processes killed at random moments, or at an exact file operation, including part way through a
  write, and again while the next process recovers what the last kill left;
- simulated errors in every kind of file operation the store makes (writes, syncs, renames,
  truncates, unlinks, directory creation, opens, reads, stats and directory listings), each with
  an errno that operation can meet on its own, and short reads and writes;
- kills and errors aimed at each kind of file of either store (packfiles, bucket files and their
  temporary and `.new` copies, level markers, pending fan-out sentinels, the compaction resume
  file, epoch files, directories), and at the store operation running when they land;
- operations and flushes dropped unfinished, as by a caller giving up on them, with their writes
  left running, and store objects dropped with writes in flight and opened again in the same
  process;
- file operations slowed, as on a slow device;
- a full disk, and a process out of file descriptors;
- heavy concurrency, with compaction and eviction running continuously alongside every read and
  write;
- packfiles filling at the size limit, fan-outs through every level, and buckets too large to read
  in one piece;
- both I/O backends, `psync` and `uring`;
- crash windows reproduced exactly from snapshots taken at the store's test pause points;
- several processes using one store directory in calls, some keeping their stores between calls
  and some opening them for each, killed at random moments or at an exact file operation, in calls
  or between them, with changes unflushed or with a fan-out commit half made, under faults on
  either store's files, and dropping operations and flushes unfinished;
- operations run outside any span, each claiming the store for itself, and inside spans, as
  commands run them.

Every kill, fault and cancellation is counted against the store operations running when it
landed and the step, the file operation and kind of file, it landed in. A kill that landed between
file operations is counted after the last one begun, as `after:<op>:<kind>`, a step of its own.
Each summary lists the operations and steps the runs reached that none hit, which a kill between
file operations does not.

## Parts

- `lore-storage/examples/store_stress/` is the harness, built against the store's `test-util`
  feature. It builds only with lore-storage's `store-stress` feature, which `stress.py build`
  enables, so `cargo test` never builds it:
  - `work <dir>` runs a randomized concurrent workload. With `--lists`, every sixteenth key is a
    fragment list over other keys, stored after its children; obliterating a live list obliterates
    them, and one already obliterated leaves them as they are. Every operation it starts and every
    one that succeeds goes to `<dir>/stress.journal`, one line written at a time, so that a kill
    at any point leaves a usable journal. It checks every payload it reads, and reports coverage:
    which operation kinds ran alongside which, what eviction and compaction reclaimed, the events
    the store logged, and the shapes the store reached.

    It also holds every read to what the run settled, as it goes: for the variants eviction may
    not take, it follows each key from the journal's expectations and each operation the run makes,
    and a read run with nothing else on its key must find the entry, the payload, or the tombstone
    that settled. A read that fails for anything but the system refusing it fails the run, as does
    a claim, and a claim the system refused is noted as `claim-refused-by-the-system`. With
    `--cancel <share>`, it drops that share of operations unfinished. With `--liveness`, once the
    run ends, it ends the shim's faults and checks that the store takes puts and flushes again in
    the same process. With `--spans <n>`, each task runs its operations in spans of up to `n`
    claimed with `hold_for_command`, as commands do, and the sampler that asks the store its
    shapes, fragment count and availability every 250 ms claims a span for each sample. With
    `--spawn-flushes <share>`, that share of flushes runs in tasks outliving their operation on the
    claim it joined. Without `--spans`, each operation claims the store for itself. After a final
    flush, it holds every bucket file to what the store holds in memory, where no simulated I/O
    errors reach the store's files.
  - `calls <dir>` runs the same workload, with the mutable store's stores, loads, swaps and lists
    added, as one of several processes using `<dir>` at once, in calls as client processes make
    them: a call claims both stores, runs a random number of operations on a random number of
    tasks, flushes, and lets its claims go, and the process pauses before the next. With
    `--keeps`, a process keeps its stores between calls, reusing their state across holds of their
    locks while their epochs are unchanged, as `LORE_STORE_RETAIN` has the client do, and ends a
    share of its calls (`--unflushed`) with the stores unflushed, which keeps their flocks until a
    later call flushes them. With `--keep-alive-ms`, it flushes and drops stores kept longer than
    that between calls, as the client's keep-alive window does. Without `--keeps`, a process opens
    its stores afresh for each call, its claims joining the claims they were opened under.
    `--cancel` drops operations and the flushes ending calls, and `--audit-files <share>` holds the
    bucket files to the store's memory after that share of the calls that flushed, where no
    simulated I/O errors reach the store's files. A call that fails before it reads the journal
    leaves the loads of the next call's claims unchecked, since a store it claimed had loaded for
    writes the next call is the first to read. `--prefill-near-limit` fills the packfiles before
    the stores open, as for `work`, and `--liveness` runs the probe as a call of its own once the
    run ends, on the kept stores or on stores opened for it, the mutable store included: a store,
    a flush and a load of it must succeed as the immutable store's puts, flush and gets must.

    The processes append to one journal, whose line order is then theirs, each store object's life
    a generation of its own. A call reads what the others appended once it has claimed the stores,
    which no other process uses until it lets them go, so what the journal settled holds for the
    whole call: every read is held to it as `work` holds reads to its own run. Every process swaps
    two shared counters up by compare-and-swap, from a value it loads or from the last one it saw,
    and a load below what the journal settled or a swap that succeeds from it is a lost update. A
    call also holds the loads its claims made to the journal: a kept store reloads at its claim
    when another process acknowledged a write to it since, never more than once, and not at all
    when no other process could have written, and stores opened afresh load once each. The
    orchestrator appends `0 g dead pid=<pid>` once a process is gone, which ends what it left
    running in the journal.
  - `verify <dir> [--deep]` checks the store against the journal and against its own files.
    - It audits the files as the kill left them, before an open recovers anything.
    - It reopens the store and loads the whole index, then checks every entry: sort order, bucket
      and group placement, flags against data, sizes, overlapping payload ranges, and packfile
      bounds.
    - It reads every payload location from the packfiles directly.
    - It reads every key under every variant, every way the store serves it, and holds the reads
      to each other and to `query`'s contract.
    - It holds tombstones to their exact flags, and in a client store every entry's durable flag
      to its variant, which is what eviction goes by.
    - It parses `.new` files no commit is pending for, which a later commit's recovery may yet
      install, and checks their entries against the packfiles.
    - Expectations come from the journal: an entry settles once a flush that started after its
      operation succeeded, and stays settled until an obliterate, or an eviction or compaction
      pass that may have taken it, ran after it.
    - `--deep` then compacts every group, flushes with a sync, reopens, and checks again.
    - Where the journal records mutable operations, it holds each mutable key to the value the
      journal settled, or to one some process stored, and `list` to the loads. It holds each
      counter to the swaps: every swap a flush settled is in its value, no two settled swaps
      started from the same value, and every increment it holds was started by some process.
  - `scenario <name>|all|list` runs deterministic scenarios. Each drives a store to an exact
    point, snapshots its directory there (what a process killed there leaves), and opens the
    snapshot. A scenario passes, fails, or reports a loss only a machine failure would expose,
    which the orchestrator lists apart. Among them: a flush fanning a group out while a compaction
    rewrites one of its packfiles, and while a size eviction walks its buckets; a compaction
    dropped with a bucket write of a rewrite or of a size eviction in flight, and the next flush
    of the bucket; and a store object dropped with a packfile or bucket write in flight and
    another opened on the directory in the same process, with and without reusing state across
    holds. Those racing compaction hold a span of the store, as a command does.
  - `retention <name>|all|list` runs deterministic interleavings of calls. Each script runs
    three `client` processes on a directory of its own, two keeping their stores and one opening
    them for each call, and sends one request at a time over their stdin, so the interleaving is
    the script's. Each reply names how many times each store loaded during the request and what
    each operation found, and the script checks both: a kept store after another process's put,
    durable put, fragment list, obliterate, copy, eviction, compaction, fan-out through every
    level, mutable store and swap; readers leaving a kept store current; a call waiting for
    another process's unflushed changes until it flushes or its delayed flush writes them, and for
    both its stores where it changed only the mutable one; a writer killed with SIGKILL with
    changes unflushed, in the middle of a call, or in the middle of a fan-out commit of either
    store, which the next claim rolls forward; a writer killed at each pause point a call reaches
    (a packfile range reserved, a payload written, a bucket write, a fan-out commit of either
    store, a packfile sync, a compaction's selection and stopped writes, a size eviction), after
    which every process reads the same; and flushes dropped part way, whose changes the next flush
    writes, and which keep the store claimed until the group flushes the store runs on after the
    drop have ended. After each script the immutable store's files are held to its structure.
  - `client <dir> [--keeps]` is the store client `retention` drives, serving one request per line
    on stdin, among them a call held at a pause point and a call whose flush is dropped at one;
    see `client.rs` for the requests.
  - `info <dir>` prints a store's layout.
- `crashpoint.c` is an LD_PRELOAD shim. It kills its process before or after the N-th file
  operation on either store, or with only part of a write done, fails operations with an errno, or
  holds them up. It counts only the operations, file kinds and running store operations it is told
  to, which the harness registers with it, and records where each counted operation, kill and
  fault fell. It keeps the running store operations and the file operations in flight in a state
  file, from which the orchestrator places a SIGKILL it sends, and the harness asks it which file
  operations are in flight when it drops an operation. See the comment at its top. The store's
  default `psync` I/O backend makes every file operation through libc. The `uring` backend bypasses
  the shim for reads and writes, so the orchestrator sets `LORE_IO_BACKEND=psync` wherever the shim
  runs, and uses `uring` for some of the rest.
- `stress.py` is the orchestrator.

## Running

From the repository root:

```sh
ulimit -n 65536
python3 scripts/store-stress/stress.py build              # shim, and the harness built twice
python3 scripts/store-stress/stress.py scenarios          # every scenario, under both builds
python3 scripts/store-stress/stress.py retention          # every retention script, under both builds
python3 scripts/store-stress/stress.py multi-process --minutes 10 --processes 6
python3 scripts/store-stress/stress.py kill-loop --minutes 30
python3 scripts/store-stress/stress.py all --minutes 20   # every mode in turn, 20 minutes each
```

`build` makes two harness builds:

- **Production, `target/stress-production/`.** Debug assertions off, as in a release. The
  workload modes use it, so that a defect shows as the damage it does rather than an abort.
- **Assertions, `target/release/`.** The `release` profile builds with debug assertions on.
  Scenarios also run under this build, where an assertion can fail first.

The modes:

| Mode | What it does |
|---|---|
| `kill-loop` | Random stores and workloads, client and server shaped, on either I/O backend, several generations per store directory, with operations in spans or outside them. Most generations are killed with SIGKILL at a random moment, some while the store opens, placed from the shim's state on `psync`; the rest end by flushing, syncing, exiting without a flush, or aborting. Each generation is verified on a copy, a quarter of them deeply, so that the next generation recovers what the kill left itself. |
| `gc-storm` | As `kill-loop`, with compaction and eviction tasks running flat out, the store's own background evictor and compactor on, tiny size and capacity caps, and groups fanning out throughout, which interrupts compaction and size eviction. |
| `crash-sweep` | Counts where a workload's file operations fall, by operation, file kind and running store operation, over a store that is empty, grown, or grown up to the packfile size limit; the workload is `work`, or one `calls` process, which uses both stores. Then kills copies of it before, after, or in the middle of operations picked evenly across those kinds. Verifies a copy, lets a second process recover the store, killing it during the recovery at times, and verifies deeply. |
| `fault-sweep` | As `crash-sweep`, failing the picked operations instead, reads, stats and listings included, with an error each meets on its own, by index or at random, sometimes as a short read or write. Checks that the store takes writes again once the faults end, verifies a copy, runs again without faults, and verifies deeply. |
| `disk-full` | Runs stores on a tmpfs of a few megabytes, in a container (`docker`, `--image` with the host's libc), until the disk fills, keeps writing, frees space, writes again, and verifies. |
| `emfile` | Runs stores under a descriptor limit low enough that opens fail, then verifies with a normal limit. |
| `scenarios` | Every deterministic scenario, under both builds, and the production build again on `uring`. |
| `retention` | Every retention script, under both builds, and the production build again on `uring`. |
| `multi-process` | Rounds of `--processes` `calls` processes on one store directory, half of them keeping their stores, some for a keep-alive window, each killed with SIGKILL at random moments and started again, the store's options random and its fan-out threshold low. Every process drops operations and flushes unfinished, and most run under the shim, which places each kill; some under its faults on either store's files, some killed by it at a file operation of a kind the round counted, and some with their file operations slowed past a keep-alive window. Each round is verified on a copy, a quarter of them deeply, and the next round carries on from what the last left. |

Useful options:

- `--minutes` is the time budget per mode.
- `--seed` replays a campaign's random choices; the summary records it.
- `--processes` is how many processes share a store in a `multi-process` round.
- `--keep` keeps scratch stores.
- `--stop-on-new` stops at the first violation class not listed in `KNOWN` in `stress.py`.

`report [<results>]` merges every campaign under a results directory: violations, reports and
harness failures with the campaigns that found them, coverage summed over all of them, and the
gaps none reached. It exits non-zero when any campaign found a violation.

Results go to `target/store-stress/results/<mode>-<time>/`:

- one directory per run, with its command lines and logs;
- a copy of the store for each run that found a violation class new to the campaign;
- `summary.txt`, with every violation class and the runs that found it, marked `NEW` or `known`,
  the coverage counters summed over the campaign, and, per store operation and step, how many
  file operations the counting runs counted and how many kills, faults and cancellations landed
  there, a kill between file operations at `after:<op>:<kind>`, the last one begun. Coverage gaps
  list combinations that never occurred, and the event gaps the store operations and steps
  counted that no kill, fault or cancellation hit: kills at file operations that change the disk,
  which a kill between file operations hits none of, cancellations in operations a caller may
  drop. `report` gives the same per mode.

A single run by hand:

```sh
B=target/stress-production/release/examples/store_stress
$B work /tmp/s1 --seconds 20 --keys 4000 --groups 4 --fan-out-threshold 8 --gc-tasks 4
$B verify /tmp/s1 --deep
LD_PRELOAD=target/store-stress/crashpoint.so STRESS_PATH=/tmp/s1/immutable STRESS_CRASH_AT=300 \
    STRESS_TEAR=1 $B work /tmp/s1 --seconds 10
$B verify /tmp/s1
$B scenario list
$B calls /tmp/s2 --keys 400 --fan-out-threshold 3 --seconds 0      # fixes the keyspace
$B calls /tmp/s2 --keeps --seconds 20 & $B calls /tmp/s2 --seconds 20 & wait
$B verify /tmp/s2
$B retention list
```

`bench <dir>` measures the store's hot paths: puts of new payloads, puts of stored ones, gets,
reads into a caller's buffer, and queries. To hold a change to no regression, build the harness on
the base and on the change and run the two in turns on a quiet machine, several times each:

```sh
$B bench /tmp/b --tasks 8 --rounds 5 --seconds 2 [--modes put-new,get] [--new-per-round 20000]
```

`work` takes `--help`-less options. Each has a default; see `work.rs`, `model.rs` and
`stress.py` (`random_store_options`, `random_run_options`). `calls` takes `work`'s and its own,
see `calls.rs` and `calls_options` in `stress.py`. Options naming the keyspace
(`--seed`, `--keys`, `--groups`, `--partitions`, `--contexts`, `--evictable-partitions`,
`--max-payload`, `--real-hash`, `--server`, `--fan-out-threshold`, `--initial-level`, `--lists`)
are fixed in
`<dir>/stress.cfg` the first time a directory is used.

## Known defects

`KNOWN` in `stress.py` maps the violation classes of a defect being worked on to it, so that they
do not drown new findings; the orchestrator labels those classes `known`.
