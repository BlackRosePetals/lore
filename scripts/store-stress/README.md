# Local store stress suite

A stress and crash suite for the local immutable store (`lore-storage/src/local/immutable_store.rs`
and `lore-storage/src/packstore.rs`), run by hand when the store changes, not in CI. It runs on
Linux.

It looks for three things:

- **Corruption.** Every payload read back, and every payload location read straight from the
  packfiles, must be the bytes stored for that hash.
- **Loss.** An entry a flush acknowledged must survive the process being killed afterwards.
- **Broken guarantees.** An obliterated association must stay obliterated, `put` must refuse
  invalid fragments, and an entry's flags must agree with its data.

It builds every extreme that arises on its own, short of a machine failure. Nothing outside the
store writes over or removes its files. A power loss or kernel crash is out of scope for the runs;
a durability gap only such a failure would expose is reported, not tested for.

- processes killed at random moments, or at an exact file operation, including part way through a
  write, and again while the next process recovers what the last kill left;
- errors injected into every kind of file operation the store makes (writes, syncs, renames,
  truncates, unlinks, directory creation, opens, reads, stats and directory listings), each with
  an errno that operation can meet on its own, and short reads and writes;
- kills and errors aimed at each kind of store file (packfiles, bucket files and their temporary
  and `.new` copies, level markers, pending fan-out sentinels, the compaction resume file,
  directories), and at the store operation running when they land;
- operations dropped unfinished, as by a caller giving up on them;
- a full disk, and a process out of file descriptors;
- heavy concurrency, with compaction and eviction running continuously alongside every read and
  write;
- packfiles filling at the size limit, fan-outs through every level, and buckets too large to read
  in one piece;
- both I/O backends, `psync` and `uring`;
- crash windows reproduced exactly from snapshots taken at the store's test pause points.

## Parts

- `lore-storage/examples/store_stress/` is the harness, built against the store's `test-util`
  feature:
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
    that settled. A read that fails for anything but the system refusing it fails the run. With
    `--cancel <share>`, it drops that share of operations unfinished. With `--liveness`, once the
    run ends, it ends the shim's faults and checks that the store takes puts and flushes again in
    the same process.
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
  - `scenario <name>|all|list` runs deterministic scenarios. Each drives a store to an exact
    point, snapshots its directory there (what a process killed there leaves), and opens the
    snapshot. A scenario passes, fails, or reports a loss only a machine failure would expose,
    which the orchestrator lists apart.
  - `info <dir>` prints a store's layout.
- `crashpoint.c` is an LD_PRELOAD shim. It kills its process before or after the N-th file
  operation on the store, or with only part of a write done, or fails operations with an errno.
  It counts only the operations, file kinds and running store operations it is told to, which the
  harness registers with it, and records where each counted operation, kill and fault fell. See
  the comment at its top. The store's default `psync` I/O backend makes every file operation
  through libc. The `uring` backend bypasses the shim for reads and writes, so the orchestrator
  sets `LORE_IO_BACKEND=psync` wherever the shim runs, and uses `uring` for some of the rest.
- `stress.py` is the orchestrator.

## Running

From the repository root:

```sh
ulimit -n 65536
python3 scripts/store-stress/stress.py build              # shim, and the harness built twice
python3 scripts/store-stress/stress.py scenarios          # every scenario, under both builds
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
| `kill-loop` | Random stores and workloads, client and server shaped, on either I/O backend, several generations per store directory. Most generations are killed with SIGKILL at a random moment, some while the store opens; the rest end by flushing, syncing, exiting without a flush, or aborting. Each generation is verified on a copy, a quarter of them deeply, so that the next generation recovers what the kill left itself. |
| `gc-storm` | As `kill-loop`, with compaction and eviction tasks running flat out, the store's own background evictor and compactor on, and tiny size and capacity caps. |
| `crash-sweep` | Counts where a workload's file operations fall, by operation, file kind and running store operation, over a store that is empty, grown, or grown up to the packfile size limit. Then kills copies of it before, after, or in the middle of operations picked evenly across those kinds. Verifies a copy, lets a second process recover the store, killing it during the recovery at times, and verifies deeply. |
| `fault-sweep` | As `crash-sweep`, failing the picked operations instead, reads, stats and listings included, with an error each meets on its own, by index or at random, sometimes as a short read or write. Checks that the store takes writes again once the faults end, verifies a copy, runs again without faults, and verifies deeply. |
| `disk-full` | Runs stores on a tmpfs of a few megabytes, in a container (`docker`, `--image` with the host's libc), until the disk fills, keeps writing, frees space, writes again, and verifies. |
| `emfile` | Runs stores under a descriptor limit low enough that opens fail, then verifies with a normal limit. |
| `scenarios` | Every deterministic scenario, under both builds, and the production build again on `uring`. |

Useful options:

- `--minutes` is the time budget per mode.
- `--seed` replays a campaign's random choices.
- `--keep` keeps scratch stores.
- `--stop-on-new` stops at the first violation class not listed in `KNOWN` in `stress.py`.

`report [<results>]` merges every campaign under a results directory: violations, reports and
harness failures with the campaigns that found them, coverage summed over all of them, and the
gaps none reached. It exits non-zero when any campaign found a violation.

Results go to `target/store-stress/results/<mode>-<time>/`:

- one directory per run, with its command lines and logs;
- a copy of the store for each run that found a violation class new to the campaign;
- `summary.txt`, with every violation class and the runs that found it, marked `NEW` or `known`,
  the coverage counters summed over the campaign, and where the sweeps' kills and faults landed.
  Coverage gaps list combinations that never occurred, and the placement lines the kinds of file
  operation a counting run saw that no kill or fault hit.

A single run by hand:

```sh
B=target/stress-production/release/examples/store_stress
$B work /tmp/s1 --seconds 20 --keys 4000 --groups 4 --fan-out-threshold 8 --gc-tasks 4
$B verify /tmp/s1 --deep
LD_PRELOAD=target/store-stress/crashpoint.so STRESS_PATH=/tmp/s1/immutable STRESS_CRASH_AT=300 \
    STRESS_TEAR=1 $B work /tmp/s1 --seconds 10
$B verify /tmp/s1
$B scenario list
```

`bench <dir>` measures the store's hot paths: puts of new payloads, puts of stored ones, gets,
reads into a caller's buffer, and queries. To hold a change to no regression, build the harness on
the base and on the change and run the two in turns on a quiet machine, several times each:

```sh
$B bench /tmp/b --tasks 8 --rounds 5 --seconds 2 [--modes put-new,get] [--new-per-round 20000]
```

`work` takes `--help`-less options. Each has a default; see `work.rs`, `model.rs` and
`stress.py` (`random_store_options`, `random_run_options`). Options naming the keyspace
(`--seed`, `--keys`, `--groups`, `--partitions`, `--contexts`, `--evictable-partitions`,
`--max-payload`, `--real-hash`, `--server`, `--fan-out-threshold`, `--initial-level`, `--lists`)
are fixed in
`<dir>/stress.cfg` the first time a directory is used.

## Known defects

None open. `KNOWN` in `stress.py` maps the violation classes of a defect being worked on to it, so
that they do not drown new findings; the orchestrator labels those classes `known`.
