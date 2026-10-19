#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 Epic Games, Inc.
# SPDX-License-Identifier: MIT
"""Drive the local store stress suite: kill loops, crash and fault sweeps, disk-full and
descriptor-exhaustion runs, GC storms, deterministic scenarios, and processes sharing a store in
calls, scripted and at random, each checked by the harness's verifier.

Run by hand, on Linux; see README.md beside this file. Every campaign writes to
<results>/<campaign>-<time>/: one directory per run with the command lines and logs, a copy of the
store for each run that found a violation class not seen before in the campaign, and summary.txt
with violations, coverage and where kills and faults landed, across every run.
"""

import argparse
import json
import os
import random
import shutil
import signal
import struct
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent

# Classes the verifier reports for defects already known, kept apart from new findings: class name
# to the defect. Empty while none is open.
KNOWN = {}


# Coverage events only a machine failure or a damaged file produces, which no run is expected to
# reach: an index pointing past what its packfile holds, a bucket file that does not parse.
MACHINE_FAILURE_ONLY = {"event lost-payload-locations-cleared", "event corrupt-bucket-reset"}

# The file operations the shim counts, those that change the disk first.
MUTATIONS = ["write", "sync", "rename", "truncate", "unlink", "mkdir"]
ALL_OPS = MUTATIONS + ["open", "read", "stat", "list"]

# The errors each kind of file operation meets on its own, with nothing outside the store at work:
# a full disk or quota, a failing device or a file system remounted read-only, memory or
# descriptors running out, a signal on a file system that lets one interrupt it. The shim maps an
# errno an operation cannot meet to EIO.
NATURAL_ERRNOS = {
    "write": ["ENOSPC", "EDQUOT", "EIO", "EROFS", "EINTR"],
    "sync": ["ENOSPC", "EDQUOT", "EIO"],
    "rename": ["ENOSPC", "EDQUOT", "EIO", "EROFS"],
    "truncate": ["EIO", "EROFS", "EINTR"],
    "unlink": ["EIO", "EROFS", "ENOSPC"],
    "mkdir": ["ENOSPC", "EDQUOT", "EIO", "EROFS", "EMLINK"],
    "open": ["EMFILE", "ENFILE", "ENOMEM", "EIO", "EINTR", "ENOSPC", "EDQUOT"],
    "read": ["EIO", "EINTR"],
    "stat": ["EIO", "ENOMEM"],
    "list": ["EIO", "EMFILE", "ENFILE", "ENOMEM"],
}


def known(name):
    """What a violation class is known as, or None for a new one."""
    return KNOWN.get(name.removeprefix("online-"))


def copy_store(source, destination):
    """Copy a store directory, keeping sparse packfiles sparse."""
    shutil.rmtree(destination, ignore_errors=True)
    subprocess.run(["cp", "-a", "--sparse=always", str(source), str(destination)], check=True)


def read_places(path):
    """The shim's placement file: (counted or failed, op, class, running operations) to count."""
    places = Counter()
    try:
        lines = Path(path).read_text().splitlines()
    except OSError:
        return places
    for line in lines:
        parts = line.split()
        if len(parts) == 5 and parts[4].isdigit():
            places[(parts[0], parts[1], parts[2], parts[3])] += int(parts[4])
    return places


def strata(places):
    """Per operation and file kind, how many were counted, and how many with each store operation
    running."""
    totals = Counter()
    during = defaultdict(Counter)
    for (kind, op, file_class, running), count in places.items():
        if kind != "op":
            continue
        totals[(op, file_class)] += count
        for name in running.split("+"):
            during[(op, file_class)][name] += count
    return totals, during


# Where the shim's state file, see `struct state` in crashpoint.c, holds each field.
STATE_KINDS, STATE_OPS, STATE_CLASSES = 47, 10, 32
STATE_LAYOUT = {"kind_count": 8, "kind_names": 16, "op_names": 1144, "class_names": 1264, "last_op": 2040,
                "running": 2048, "in_flight": 2424, "size": 3704}


def read_state(path):
    """What a process the shim watched left in its state file as it died: the store operations
    running, `idle` for none, and the file operations in flight, `op:kind`, or when none was, the
    last one begun as `after:op:kind`, which is no step a kill landed in. None without a state."""
    try:
        data = Path(path).read_bytes()
    except OSError:
        return None
    if len(data) < STATE_LAYOUT["size"] or not data.startswith(b"STRESS1"):
        return None

    def names(offset, count, width):
        return [data[offset + width * index:offset + width * (index + 1)].split(b"\0")[0].decode(errors="replace")
                for index in range(count)]

    kinds = names(STATE_LAYOUT["kind_names"], struct.unpack_from("<I", data, STATE_LAYOUT["kind_count"])[0], 24)
    ops = names(STATE_LAYOUT["op_names"], STATE_OPS, 12)
    classes = names(STATE_LAYOUT["class_names"], STATE_CLASSES, 24)
    running = struct.unpack_from(f"<{STATE_KINDS}Q", data, STATE_LAYOUT["running"])
    operations = [kind for kind, count in zip(kinds, running) if count] or ["idle"]
    in_flight = struct.unpack_from(f"<{STATE_OPS * STATE_CLASSES}I", data, STATE_LAYOUT["in_flight"])
    steps = [f"{ops[index // STATE_CLASSES]}:{classes[index % STATE_CLASSES]}"
             for index, count in enumerate(in_flight) if count]
    last_op, last_class = struct.unpack_from("<II", data, STATE_LAYOUT["last_op"])
    if not steps and last_op and last_class:
        steps = [f"after:{ops[last_op - 1]}:{classes[last_class - 1]}"]
    return operations, steps or ["no-file-op"]


# The operations a caller may drop unfinished: `work`'s kinds but those it never drops, and a
# call's flush, which `calls` tracks as `end`.
CANCELLABLE = {"put", "putf", "putm", "get", "getinto", "meta", "query", "copy", "ob", "flush", "flushsync",
               "compact", "evict", "loadindex", "verify", "mstore", "mload", "mswap", "mlist", "end"}


def eligible(event, operation, step):
    """Whether `event` can land in `operation` at `step`: a kill counts at a file operation that
    changes the disk, never at `after:op:kind`, between file operations, and a cancellation in an
    operation a caller may drop."""
    if event == "kill":
        return step.split(":")[0] in MUTATIONS
    if event == "cancel":
        return operation in CANCELLABLE
    return True


def event_lines(events):
    """Report `events`, keyed by event, store operation, step and mode: per store operation and
    step, the file operations counted and where each kill, fault and cancellation landed, by mode,
    then, where any file operations were counted, per event any landed, the operations and steps
    counted that none hit. A kill between file operations is listed at `after:op:kind`, a step of
    its own, so that it hits none counted."""
    rows = defaultdict(lambda: defaultdict(Counter))
    for (event, operation, step, mode), count in events.items():
        rows[(operation, step)][event][mode] += count
    if not rows:
        return []
    lines = ["events by store operation and step (seen: file operations counted; kill, fault, cancel: where "
             "each landed), by mode:"]
    for (operation, step), by_event in sorted(rows.items()):
        cells = [f"{event} " + " ".join(f"{mode}={count}" for mode, count in sorted(by_event[event].items()))
                 for event in ["seen", "kill", "fault", "cancel"] if event in by_event]
        lines.append(f"  {operation} {step}: " + "; ".join(cells))
    seen = {key for key, by_event in rows.items() if "seen" in by_event}
    for event in ["kill", "fault", "cancel"]:
        hit = {key for key, by_event in rows.items() if event in by_event}
        if hit and seen:
            missed = sorted(f"{operation} {step}" for operation, step in seen - hit if eligible(event, operation, step))
            lines.append(f"{event} gaps, counted and never hit ({len(missed)}): {', '.join(missed) or 'none'}")
    return lines


class Campaign:
    def __init__(self, args, name):
        self.args = args
        self.binary = Path(args.binary)
        # The process id keeps two campaigns of a mode started together apart.
        self.mode = name
        self.root = Path(args.results) / f"{name}-{time.strftime('%Y%m%d-%H%M%S')}-{os.getpid()}"
        self.root.mkdir(parents=True, exist_ok=True)
        self.scratch = Path(args.scratch) / self.root.name
        self.scratch.mkdir(parents=True, exist_ok=True)
        self.runs = 0
        self.violations = Counter()
        self.violation_runs = defaultdict(list)
        self.saved_classes = set()
        self.coverage = Counter()
        self.coverage_runs = 0
        self.notes = Counter()
        self.failures = Counter()
        # Per event, the store operation it landed in and the step, `op:kind`, or `after:op:kind`
        # for a kill between file operations: `seen` for the file operations counting runs
        # counted, and `kill`, `fault` and `cancel` for where each simulated crash, I/O error and
        # cancellation landed.
        self.events = Counter()
        # Losses only a machine failure would expose, which the suite reports but does not fail.
        self.reports = {}
        self.started = time.time()
        self.rng = random.Random(args.seed)
        print(f"campaign {self.root} seed {args.seed}", flush=True)

    def out_of_time(self):
        return time.time() - self.started >= self.args.minutes * 60

    def run_dir(self, label):
        self.runs += 1
        path = self.root / f"{self.runs:05d}-{label}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def command(self, *args):
        return [str(self.binary), *[str(arg) for arg in args]]

    def start(self, command, log, env=None, prefix=None):
        """Start `command`, its command line and output to `log`, and return the process."""
        full_env = dict(os.environ)
        full_env.setdefault("LORE_IO_BACKEND", "psync")
        if env:
            full_env.update(env)
        if prefix:
            command = [*prefix, *command]
        with open(log, "ab") as output:
            output.write((" ".join(command) + "\n").encode())
            if env:
                output.write((json.dumps(env) + "\n").encode())
            output.flush()
            return subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, env=full_env)

    def run(self, command, log, env=None, timeout=None, kill_after=None, prefix=None):
        """Run `command`, its output to `log`, killing it with SIGKILL after `kill_after` seconds.
        Returns (exit code or -signal, killed by us)."""
        process = self.start(command, log, env, prefix)
        killed = False
        deadline = time.time() + (timeout or 3600)
        while True:
            try:
                code = process.wait(timeout=0.02)
                break
            except subprocess.TimeoutExpired:
                pass
            now = time.time()
            if kill_after is not None and now - (deadline - (timeout or 3600)) >= kill_after:
                process.send_signal(signal.SIGKILL)
                killed = True
                kill_after = None
            if now >= deadline:
                process.send_signal(signal.SIGKILL)
                process.wait()
                return (-9, False)
        return (code, killed)

    def verify(self, store, run_dir, deep=False, label="verify", copy=False):
        """Check `store`, or with `copy` a copy of it, leaving the store as the kill left it for the
        next process to recover."""
        log = run_dir / f"{label}.log"
        target = store
        if copy:
            target = self.scratch / "verify-copy"
            copy_store(store, target)
        args = ["verify", target]
        if deep:
            args.append("--deep")
        code, _ = self.run(self.command(*args), log, timeout=1800)
        classes = self.parse_verify(log)
        if code not in (0, 1) and not classes:
            classes = {f"verify-crashed-{code}": 1}
        self.record(run_dir, store, classes)
        if copy:
            shutil.rmtree(target, ignore_errors=True)
        return classes

    def parse_verify(self, log):
        classes = {}
        for line in Path(log).read_text(errors="replace").splitlines():
            if line.startswith("VIOLATION "):
                name, _, rest = line[len("VIOLATION "):].partition(":")
                count = int(rest.split()[0]) if rest.split() and rest.split()[0].isdigit() else 1
                classes[name] = classes.get(name, 0) + count
            elif line.startswith("NOTE "):
                name, _, rest = line[len("NOTE "):].partition(":")
                count = int(rest.split()[0]) if rest.split() and rest.split()[0].isdigit() else 1
                self.notes[name] += count
        return classes

    def parse_work(self, log, run_dir):
        """Take a work log's coverage and findings, and return whether its journal lost a line."""
        text = Path(log).read_text(errors="replace")
        covered = False
        journal_lost = False
        for line in text.splitlines():
            if line.startswith("COVERAGE overlap "):
                _, _, pair, count = line.split()
                self.coverage[f"overlap {pair}"] += int(count)
                covered = True
            elif line.startswith("COVERAGE event "):
                _, _, event, count = line.split()
                self.coverage[f"event {event}"] += int(count)
            elif line.startswith("COVERAGE gc "):
                parts = line.split()[2:]
                for name, value in zip(["eviction-passes", "evicted", "compaction-passes", "compacted-bytes"], parts):
                    self.coverage[f"gc {name}"] += int(value)
            elif line.startswith("COVERAGE lists "):
                parts = line.split()[2:]
                for name, value in zip(["stored", "obliterated", "refused-without-payload", "refused-otherwise"], parts):
                    # Obliterate takes a list it cannot load as done, so the refusal is never seen.
                    if name != "refused-without-payload":
                        self.coverage[f"lists {name}"] += int(value)
            elif line.startswith("COVERAGE calls "):
                _, _, name, count = line.split()
                self.coverage[f"calls {name}"] += int(count)
            elif line.startswith("COVERAGE cancel "):
                _, event, operation, step, count = line.split()
                self.events[(event, operation, step)] += int(count)
            elif line.startswith("COVERAGE shapes "):
                parts = line.split()[2:]
                for name, value in zip(["max-level", "max-packfiles", "max-writeable", "settled", "max-unsettled"], parts):
                    key = f"shape {name}"
                    self.coverage[key] = max(self.coverage[key], int(value))
            elif line.startswith("probe after "):
                self.coverage["liveness probes"] += 1
            elif line.startswith("REPORT "):
                name, _, detail = line[len("REPORT "):].partition(": ")
                self.reports.setdefault(name, detail)
            elif line.startswith("HANG"):
                self.record(run_dir, None, {"hang": 1})
            elif line.startswith("STORAGE-STALL"):
                self.notes["storage-stall"] += 1
            elif line.startswith("NOTE "):
                self.notes[line[len("NOTE "):].partition(":")[0]] += 1
            elif line.startswith("JOURNAL-LOST"):
                self.failures["journal-lost"] += 1
                journal_lost = True
            elif "panicked at" in line:
                self.record(run_dir, None, {"panic: " + line.split("panicked at")[-1].strip()[:80]: 1})
        if covered:
            self.coverage_runs += 1
        return journal_lost

    def place(self, places):
        """Count the file operations a shim's placement file says it counted, by the store
        operations running, and where its faults landed."""
        for (kind, op, file_class, running), count in places.items():
            for name in running.split("+"):
                self.events[("seen" if kind == "op" else "fault", name, f"{op}:{file_class}")] += count

    def note_kill(self, killed_file):
        """Count where a kill the shim made landed, from its killed file, returning whether it
        made one."""
        try:
            op, file_class, running, *_ = Path(killed_file).read_text().split()
        except (OSError, ValueError):
            return False
        for name in running.split("+"):
            self.events[("kill", name, f"{op}:{file_class}")] += 1
        return True

    def note_state_kill(self, state_file):
        """Count where a SIGKILL from outside landed, from the state file the shim left."""
        state = read_state(state_file)
        if state is None:
            self.notes["kill unplaced"] += 1
            return
        operations, steps = state
        for name in operations:
            for step in steps:
                self.events[("kill", name, step)] += 1

    def record(self, run_dir, store, classes):
        new = False
        for name, count in classes.items():
            self.violations[name] += count
            self.violation_runs[name].append(run_dir.name)
            if name not in self.saved_classes:
                self.saved_classes.add(name)
                new = True
        if new and store is not None and Path(store).exists():
            target = run_dir / "store"
            if not target.exists():
                copy_store(store, target)
        if classes:
            unknown = [name for name in classes if known(name) is None]
            marker = "NEW" if unknown else "known"
            print(f"  {run_dir.name}: {marker} {dict(classes)}", flush=True)

    def event_lines(self):
        return event_lines({(event, operation, step, self.mode): count
                            for (event, operation, step), count in self.events.items()})

    def summary(self):
        lines = [f"campaign {self.root.name}: {self.runs} runs in {(time.time() - self.started) / 60:.1f} min, "
                 f"seed {self.args.seed}"]
        lines.append("violations (runs listed for the first few):")
        for name, count in sorted(self.violations.items(), key=lambda item: -item[1]):
            known_as = known(name)
            label = f"known: {known_as}" if known_as else "NEW"
            runs = self.violation_runs[name]
            lines.append(f"  {name}: {count} in {len(runs)} runs [{label}] e.g. {', '.join(runs[:4])}")
        if not self.violations:
            lines.append("  none")
        if self.notes:
            lines.append("notes: " + ", ".join(f"{name} {count}" for name, count in sorted(self.notes.items())))
        if self.reports:
            lines.append("reported, a machine failure or a retryable error only:")
            for name, detail in sorted(self.reports.items()):
                lines.append(f"  {name}: {detail[:300]}")
        lines.append(f"coverage over {self.coverage_runs} workload runs:")
        gaps = []
        for name, count in sorted(self.coverage.items()):
            lines.append(f"  {name}: {count}")
            if count == 0 and not name.startswith("shape") and name not in MACHINE_FAILURE_ONLY:
                gaps.append(name)
        lines.append("coverage gaps: " + (", ".join(gaps) if gaps else "none"))
        lines.extend(self.event_lines())
        text = "\n".join(lines)
        (self.root / "summary.txt").write_text(text + "\n")
        (self.root / "summary.json").write_text(json.dumps({
            "runs": self.runs,
            "minutes": (time.time() - self.started) / 60,
            "violations": dict(self.violations),
            "violation_runs": {name: runs[:4] for name, runs in self.violation_runs.items()},
            "failures": dict(self.failures),
            "notes": dict(self.notes),
            "reports": self.reports,
            "coverage": dict(self.coverage),
            "coverage_runs": self.coverage_runs,
            "events": [[*key, count] for key, count in sorted(self.events.items())],
        }, indent=1))
        print(text, flush=True)


def report(results):
    """Merge the summaries of every campaign under `results`: violations, reports and failures
    with the campaigns that found them, coverage summed, and the gaps no campaign reached."""
    violations, failures, notes, reports, coverage, events = Counter(), Counter(), Counter(), {}, Counter(), Counter()
    found_in = defaultdict(list)
    runs = coverage_runs = 0
    campaigns = sorted(Path(results).glob("*/summary.json"))
    for path in campaigns:
        summary = json.loads(path.read_text())
        name = path.parent.name
        runs += summary["runs"]
        coverage_runs += summary["coverage_runs"]
        for kind, count in summary["violations"].items():
            violations[kind] += count
            found_in[kind].append(name)
        failures.update(summary["failures"])
        notes.update(summary.get("notes", {}))
        reports.update(summary["reports"])
        mode = name.rsplit("-", 3)[0]
        for event, operation, step, count in summary.get("events", []):
            events[(event, operation, step, mode)] += count
        for kind, count in summary["coverage"].items():
            if kind.startswith("shape"):
                coverage[kind] = max(coverage[kind], count)
            else:
                coverage[kind] += count
    lines = [f"{len(campaigns)} campaigns, {runs} runs, coverage from {coverage_runs} workload runs"]
    lines.append("violations:")
    for kind, count in sorted(violations.items(), key=lambda item: -item[1]):
        label = f"known: {known(kind)}" if known(kind) else "NEW"
        lines.append(f"  {kind}: {count} [{label}] in {', '.join(found_in[kind][:4])}")
    if not violations:
        lines.append("  none")
    lines.append("harness failures: " + (", ".join(f"{k} {v}" for k, v in failures.items()) or "none"))
    lines.append("notes: " + (", ".join(f"{k} {v}" for k, v in sorted(notes.items())) or "none"))
    lines.append("reported, a machine failure or a retryable error only:")
    for kind, detail in sorted(reports.items()):
        lines.append(f"  {kind}: {detail[:300]}")
    if not reports:
        lines.append("  none")
    lines.append("coverage:")
    for kind, count in sorted(coverage.items()):
        lines.append(f"  {kind}: {count}")
    gaps = [kind for kind, count in sorted(coverage.items())
            if count == 0 and not kind.startswith("shape") and kind not in MACHINE_FAILURE_ONLY]
    lines.append("coverage gaps: " + (", ".join(gaps) if gaps else "none"))
    lines.extend(event_lines(events))
    print("\n".join(lines))
    return 1 if violations or failures else 0


def random_store_options(rng, profile):
    """Options fixed when a store directory is first used."""
    big = rng.random() < 0.1
    options = [
        "--seed", rng.randrange(1, 1 << 30),
        "--keys", 20000 if big else rng.choice([300, 2000, 6000, 20000]),
        "--groups", rng.choice(["1", "2"]) if big else rng.choice(["1", "2", "4", "16", "all"]),
        # Thresholds this high grow buckets past the size read in one piece.
        "--fan-out-threshold", rng.choice([4000, 8000]) if big else rng.choice([3, 8, 24, 100, 1000]),
        "--max-payload", rng.choice(["2k", "32k", "128k", "256k"]),
        "--partitions", rng.choice([1, 2, 3, 4]),
        "--contexts", rng.choice([1, 2, 3]),
        "--evictable-partitions", rng.choice([0, 1, 2]),
    ]
    if profile == "server" or (profile != "client" and rng.random() < 0.25):
        options.append("--server")
    if rng.random() < 0.1:
        options.append("--real-hash")
    if rng.random() < 0.5:
        options.append("--lists")
    if profile == "gc-storm":
        # Groups fanning out throughout the run, which interrupts compaction and size eviction.
        options += ["--keys", rng.choice([6000, 20000]), "--groups", rng.choice(["16", "all"]),
                    "--fan-out-threshold", rng.choice([2, 3, 4])]
    return options


MIXES = [
    "",
    "compact=6,evict=6",
    "ob=12",
    "flush=8,flushsync=4",
    "putf=10,copy=15",
    "putm=10,copy=10,ob=6",
    "get=60,getinto=20,put=10",
    "compact=3,evict=3,ob=6,putf=4,copy=8,flush=4",
    "loadindex=2,stopgc=3,compact=4",
    "meta=20,query=20,put=10,ob=4,evict=2",
]


def random_run_options(rng, profile):
    """Options a single run of an existing store directory varies."""
    options = [
        "--tasks", rng.choice([1, 2, 8, 16, 32, 64]),
        "--gc-tasks", rng.choice([0, 1, 2, 4, 6] if profile != "gc-storm" else [4, 6, 8]),
        "--hot-keys", rng.choice([4, 48, 512]),
        "--hot-share", rng.choice([0.0, 0.3, 0.8]),
        "--flush-background", rng.choice(["true", "false"]),
        "--flush-delay", rng.choice([0, 1, 3, 3600, 3600]),
        "--threads", rng.choice([2, 4, 8, 16]),
        "--run-seed", rng.randrange(1, 1 << 30),
        "--compact-max-size", rng.choice(["1", "1,256k,4m,64m", "64k", "16m"]),
        "--evict-max-capacity", rng.choice(["1", "1,4096,1m", "100"]),
    ]
    mix = rng.choice(MIXES)
    if profile == "gc-storm":
        mix = "compact=8,evict=8,stopgc=1,put=40,flush=6," + mix
    if rng.random() < 0.2:
        mix += ",failwrite=0.3,failsync=0.05"
    if mix:
        options += ["--mix", mix.strip(",")]
    if rng.random() < 0.3 or profile == "gc-storm":
        options += ["--background-gc", "--gc-max-size", rng.choice(["1", "1m", "64m"]),
                    "--gc-max-capacity", rng.choice(["1", "1000", "100000"]), "--gc-delay-ms", rng.choice([50, 300, 2000])]
    if rng.random() < 0.2:
        options.append("--load-index")
    if rng.random() < 0.1:
        options += ["--prefill-near-limit", rng.choice(["1m", "8m", "64m"])]
    if rng.random() < 0.15:
        options.append("--sync-gc")
    if rng.random() < 0.5:
        # Flushes one at a time, which lets the verifier hold every flush to what it covers.
        options.append("--serialize-flushes")
    if rng.random() < 0.25:
        # Callers giving up on operations, which drops them unfinished.
        options += ["--cancel", rng.choice([0.01, 0.05, 0.2]), "--cancel-us", rng.choice([50, 500, 5000])]
    if rng.random() < 0.5:
        # Operations in spans of a claim, as commands run them, and flushes outliving them.
        options += ["--spans", rng.choice([1, 8, 64]), "--spawn-flushes", rng.choice([0, 0.3])]
    options += ["--end", rng.choice(["flush", "flush", "sync", "drop", "exit", "abort"])]
    return options


def backend(rng, share=0.25):
    """The I/O backend for a run the shim does not watch: now and then uring, which the shim
    cannot see into."""
    return {"LORE_IO_BACKEND": "uring" if rng.random() < share else "psync"}


def watched(campaign, store, state=None, **env):
    """The environment that runs a process under the shim, watching the files of both stores in
    `store`, leaving its state in `state`, with the shim's other settings `env`."""
    watch = {"LORE_IO_BACKEND": "psync", "LD_PRELOAD": str(campaign.args.shim),
             "STRESS_PATH": f"{store / 'immutable'},{store / 'mutable'}"}
    if state:
        watch["STRESS_STATE"] = str(state)
    watch.update({name: str(value) for name, value in env.items()})
    return watch


def kill_loop(campaign, profile="mixed"):
    """Generations of random work on random stores, each killed at a random moment or left to end,
    each checked on a copy, so that the next generation recovers what the kill left itself."""
    args = campaign.args
    rng = campaign.rng
    while not campaign.out_of_time():
        store = campaign.scratch / f"store-{campaign.runs + 1:05d}"
        store_options = random_store_options(rng, profile)
        # Hashes the store can check: a write read back and rehashed, and a verify that heals.
        real_hash = "--real-hash" in store_options
        generations = rng.randint(1, args.generations)
        for generation in range(generations):
            if campaign.out_of_time():
                break
            run_dir = campaign.run_dir(f"{profile}-g{generation}")
            seconds = rng.choice([1, 3, 8, 20])
            options = (store_options if generation == 0 else []) + random_run_options(rng, profile)
            options += ["--seconds", seconds]
            env = backend(rng)
            if env["LORE_IO_BACKEND"] == "psync":
                env = watched(campaign, store, run_dir / "state")
            if real_hash and rng.random() < 0.3:
                env["LORE_IMMUTABLE_STORE_VERIFY_WRITE"] = "1"
            if real_hash and rng.random() < 0.2:
                options.append("--heal")
            roll = rng.random()
            if roll < 0.2 and generation > 0:
                # Early, while the store opens and recovers what the last kill left.
                kill_after = rng.uniform(0.01, 0.4)
            elif roll < 0.8:
                kill_after = rng.uniform(0.2, seconds + 3)
            else:
                kill_after = None
            code, killed = campaign.run(campaign.command("work", store, *options), run_dir / "work.log",
                                        env=env, timeout=seconds + 600, kill_after=kill_after)
            if killed and "STRESS_STATE" in env:
                campaign.note_state_kill(env["STRESS_STATE"])
            if campaign.parse_work(run_dir / "work.log", run_dir):
                # Judged against an incomplete history, the store would show losses it never had.
                break
            if code not in (0, 1) and not killed and code not in (-6, 134):
                campaign.failures[f"work-exit-{code}"] += 1
                if code == 3:
                    campaign.record(run_dir, store, {"work-failed-to-start": 1})
            deep = rng.random() < 0.25
            classes = campaign.verify(store, run_dir, deep=deep, copy=True)
            if classes and args.stop_on_new and any(known(name) is None for name in classes):
                return
        if not args.keep:
            shutil.rmtree(store, ignore_errors=True)


def prepare_base(campaign, base, run_dir, rng):
    """A store to sweep over, and the workload the sweep runs on it: `work`, or now and then one
    `calls` process, which uses the mutable store beside the immutable one. Empty at times, which
    leaves the windows of a group's first writes to the sweep, otherwise grown for a while, now
    and then up to the packfile size limit. Returns the workload, and the store options of an
    empty store, which the sweep's first run passes."""
    shutil.rmtree(base, ignore_errors=True)
    workload = rng.choice(["work", "work", "calls"])
    store_options = random_store_options(rng, "mixed")
    if rng.random() < 0.25:
        base.mkdir(parents=True)
        return workload, store_options
    prefill = ["--prefill-near-limit", rng.choice(["1m", "8m"])] if rng.random() < 0.3 else []
    campaign.run(campaign.command(workload, base, *store_options, *prefill, "--seconds", rng.choice([1, 2]),
                                  "--end", "flush"), run_dir / "prepare.log", timeout=600)
    return workload, []


def workload_options(rng, workload):
    """The options of one run of `workload` over an existing store directory."""
    if workload == "calls":
        return calls_options(rng, rng.random() < 0.5)
    return random_run_options(rng, "mixed")


def count_operations(campaign, store, workload, options, run_dir, ops):
    """Run `workload` once under the shim counting its file operations of kinds `ops`, returning
    where they fell."""
    places_file = run_dir / "places.txt"
    env = watched(campaign, store, STRESS_OPS=",".join(ops), STRESS_PLACES=places_file)
    campaign.run(campaign.command(workload, store, *options), run_dir / "count.log", env=env, timeout=600)
    places = read_places(places_file)
    campaign.place(places)
    return strata(places)


def pick_stratum(rng, totals, during):
    """An operation kind and file kind, picked evenly among those the counting run saw, with the
    store operation it must land during now and then, and how many of them there are."""
    op, file_class = rng.choice(sorted(totals))
    count = totals[(op, file_class)]
    running = None
    names = sorted(during[(op, file_class)])
    if names and rng.random() < 0.5:
        running = rng.choice(names)
        count = during[(op, file_class)][running]
    return op, file_class, running, count


def target_env(campaign, store, op, file_class, running):
    env = watched(campaign, store, STRESS_OPS=op, STRESS_CLASS=file_class)
    if running:
        env["STRESS_DURING"] = running
    return env


def crash_env(rng, op, point, killed):
    """What has the shim kill its process at the `point`-th counted operation, of kind `op`,
    before, after or part way through it, and write where to `killed`."""
    mode = rng.choice(["after", "before", "tear"] if op == "write" else ["after", "before"])
    env = {"STRESS_CRASH_AT": str(point), "STRESS_KILLED": str(killed)}
    if mode == "before":
        env["STRESS_CRASH_BEFORE"] = "1"
    elif mode == "tear":
        env["STRESS_TEAR"] = "1"
    return mode, env


def crash_sweep(campaign):
    """Kill the process right before, right after, or in the middle of file operations picked
    evenly across every kind of operation on every kind of file of either store a run makes, and
    across the store operations running then. Check a copy, then let a second process recover the
    store itself, killing it during the recovery at times, and check again."""
    args = campaign.args
    rng = campaign.rng
    while not campaign.out_of_time():
        base = campaign.scratch / "sweep-base"
        prepare = campaign.run_dir("sweep-prepare")
        workload, first_options = prepare_base(campaign, base, prepare, rng)
        options = first_options + workload_options(rng, workload) + ["--seconds", rng.choice([1, 3])]
        probe = campaign.scratch / "sweep-probe"
        copy_store(base, probe)
        totals, during = count_operations(campaign, probe, workload, options, prepare, MUTATIONS)
        shutil.rmtree(probe, ignore_errors=True)
        if not totals:
            continue
        for _ in range(args.points):
            if campaign.out_of_time():
                break
            op, file_class, running, count = pick_stratum(rng, totals, during)
            point = rng.randrange(1, count + 1)
            store = campaign.scratch / "sweep-run"
            copy_store(base, store)
            run_dir = campaign.run_dir(f"crash-{workload}-{op}-{file_class}-{point}")
            env = target_env(campaign, store, op, file_class, running)
            mode, crash = crash_env(rng, op, point, run_dir / "killed.txt")
            env.update(crash)
            campaign.run(campaign.command(workload, store, *options), run_dir / f"{workload}.log", env=env,
                         timeout=600)
            if not campaign.note_kill(run_dir / "killed.txt"):
                campaign.notes["kill missed"] += 1
            campaign.parse_work(run_dir / f"{workload}.log", run_dir)
            classes = campaign.verify(store, run_dir, copy=True)
            if not classes:
                recover(campaign, store, workload, run_dir, rng)
            shutil.rmtree(store, ignore_errors=True)


def recover(campaign, store, workload, run_dir, rng):
    """The next process carries on from whatever the kill left, recovering it itself; at times it
    is killed during that recovery too, and a third process recovers both."""
    if rng.random() < 0.4:
        op = rng.choice(["rename", "unlink", "write", "sync"])
        env = watched(campaign, store, STRESS_OPS=op, STRESS_DURING="open", STRESS_CRASH_AT=rng.randrange(1, 6),
                      STRESS_KILLED=run_dir / "killed-recovery.txt")
        campaign.run(campaign.command(workload, store, "--seconds", 1, "--end", "flush"),
                     run_dir / "recovery.log", env=env, timeout=600)
        if not campaign.note_kill(run_dir / "killed-recovery.txt"):
            campaign.notes["recovery kill missed"] += 1
        campaign.parse_work(run_dir / "recovery.log", run_dir)
    campaign.run(campaign.command(workload, store, "--seconds", 2, "--end", "flush"), run_dir / "after.log",
                 timeout=600)
    campaign.parse_work(run_dir / "after.log", run_dir)
    campaign.verify(store, run_dir, deep=True, label="verify2")


def fault_sweep(campaign):
    """Fail file operations, picked evenly across every kind of operation on every kind of file of
    either store, reads, stats and listings included, with an error each can meet on its own; the
    run goes on through them, then checks that the store takes writes again once they end. Check a
    copy, then run the store on without faults and check again."""
    args = campaign.args
    rng = campaign.rng
    while not campaign.out_of_time():
        base = campaign.scratch / "fault-base"
        prepare = campaign.run_dir("fault-prepare")
        workload, first_options = prepare_base(campaign, base, prepare, rng)
        seconds = rng.choice([2, 5])
        options = first_options + workload_options(rng, workload) + ["--seconds", seconds, "--liveness"]
        probe = campaign.scratch / "fault-probe"
        copy_store(base, probe)
        totals, during = count_operations(campaign, probe, workload, options, prepare, ALL_OPS)
        shutil.rmtree(probe, ignore_errors=True)
        if not totals:
            continue
        for _ in range(max(1, args.points // 4)):
            if campaign.out_of_time():
                break
            op, file_class, running, count = pick_stratum(rng, totals, during)
            store = campaign.scratch / "fault-run"
            copy_store(base, store)
            run_dir = campaign.run_dir(f"fault-{workload}-{op}-{file_class}")
            env = target_env(campaign, store, op, file_class, running)
            errnos = rng.sample(NATURAL_ERRNOS[op], k=min(2, len(NATURAL_ERRNOS[op])))
            env.update({"STRESS_FAIL_ERRNO": ",".join(errnos), "STRESS_SEED": str(rng.randrange(1 << 30)),
                        "STRESS_SHORT": rng.choice(["0", "1"]), "STRESS_PLACES": str(run_dir / "places.txt")})
            if rng.random() < 0.3:
                env["STRESS_FAIL_PROB"] = str(rng.choice([0.01, 0.05, 0.3]))
            else:
                env["STRESS_FAIL_AT"] = str(rng.randrange(1, count + 1))
                env["STRESS_FAIL_COUNT"] = str(rng.choice([1, 1, 3, 50, 100000]))
            kill_after = rng.uniform(0.5, seconds + 2) if rng.random() < 0.3 else None
            campaign.run(campaign.command(workload, store, *options), run_dir / f"{workload}.log", env=env,
                         timeout=seconds + 600, kill_after=kill_after)
            campaign.place(read_places(run_dir / "places.txt"))
            campaign.parse_work(run_dir / f"{workload}.log", run_dir)
            campaign.verify(store, run_dir, copy=True)
            campaign.run(campaign.command(workload, store, "--seconds", 3, "--end", "sync"), run_dir / "after.log",
                         timeout=600)
            campaign.parse_work(run_dir / "after.log", run_dir)
            campaign.verify(store, run_dir, deep=True, label="verify2")
            shutil.rmtree(store, ignore_errors=True)


def disk_full(campaign):
    """Run stores on a tmpfs of a few megabytes, in a container, until the disk fills, keep writing,
    free space, write again, and check."""
    args = campaign.args
    rng = campaign.rng
    image = args.image
    probe = subprocess.run(["docker", "run", "--rm", image, "true"], capture_output=True, text=True)
    if probe.returncode != 0:
        print(f"disk-full needs docker and the {image} image: {probe.stderr.strip()[:200]}", flush=True)
        campaign.failures["disk-full-unavailable"] += 1
        return
    binary_dir = campaign.binary.resolve().parent
    while not campaign.out_of_time():
        run_dir = campaign.run_dir("enospc")
        size = rng.choice(["4m", "16m", "48m"])
        store_options = " ".join(str(option) for option in random_store_options(rng, "mixed"))
        run_options = " ".join(f"'{option}'" for option in random_run_options(rng, "mixed"))
        seconds = rng.choice([3, 8, 15])
        filler = rng.choice([0, 1, 2, 4])
        binary = f"/stress-bin/{campaign.binary.name}"
        script = f"""
set -u
S=/full/store
# The journal stays off the full disk, so that every operation the store saw is in it.
mkdir -p $S
ln -s {run_dir}/stress.journal $S/stress.journal
ln -s {run_dir}/stress.gen $S/stress.gen
{binary} work $S {store_options} {run_options} --seconds {seconds} > {run_dir}/work.log 2>&1
echo "work exit $?" >> {run_dir}/work.log
dd if=/dev/zero of=/full/filler bs=1M count={filler} 2>/dev/null
{binary} work $S --seconds {seconds} --tasks 16 > {run_dir}/work2.log 2>&1
echo "work2 exit $?" >> {run_dir}/work2.log
rm -f /full/filler
{binary} work $S --seconds 2 --end sync > {run_dir}/work3.log 2>&1
{binary} verify $S > {run_dir}/verify.log 2>&1
echo "verify exit $?" >> {run_dir}/verify.log
# The deep check compacts and syncs, which the full disk leaves no room for: it runs on a copy.
cp -a $S {run_dir}/copy
{binary} verify {run_dir}/copy --deep > {run_dir}/verify-deep.log 2>&1
echo "verify exit $?" >> {run_dir}/verify-deep.log
if grep -q '^VIOLATION' {run_dir}/verify.log {run_dir}/verify-deep.log; then mv {run_dir}/copy {run_dir}/store; else rm -rf {run_dir}/copy; fi
"""
        command = [
            "docker", "run", "--rm", "--user", f"{os.getuid()}:{os.getgid()}",
            "--tmpfs", f"/full:rw,size={size},mode=1777",
            "-v", f"{binary_dir}:/stress-bin:ro", "-v", f"{run_dir}:{run_dir}",
            image, "sh", "-c", script,
        ]
        campaign.run(command, run_dir / "container.log", timeout=seconds * 4 + 900)
        journal_lost = False
        for log in ["work.log", "work2.log", "work3.log"]:
            if (run_dir / log).exists():
                journal_lost |= campaign.parse_work(run_dir / log, run_dir)
        if journal_lost:
            # Judged against an incomplete history, the store would show losses it never had.
            continue
        classes = {}
        for log in ["verify.log", "verify-deep.log"]:
            if (run_dir / log).exists():
                classes.update(campaign.parse_verify(run_dir / log))
            else:
                classes["enospc-run-failed"] = 1
        campaign.record(run_dir, None, classes)


def descriptor_exhaustion(campaign):
    """Run stores with a descriptor limit low enough that opens fail, then check them with a normal
    limit."""
    rng = campaign.rng
    while not campaign.out_of_time():
        store = campaign.scratch / f"emfile-{campaign.runs + 1:05d}"
        run_dir = campaign.run_dir("emfile")
        store_options = random_store_options(rng, "mixed")
        campaign.run(campaign.command("work", store, *store_options, "--seconds", 2, "--end", "flush"), run_dir / "prepare.log", timeout=600)
        limit = rng.choice([24, 40, 64, 128])
        seconds = rng.choice([2, 6])
        options = random_run_options(rng, "mixed") + ["--seconds", seconds]
        campaign.run(campaign.command("work", store, *options), run_dir / "work.log", timeout=seconds + 600,
                     prefix=["prlimit", f"--nofile={limit}:{limit}", "--"])
        campaign.parse_work(run_dir / "work.log", run_dir)
        campaign.verify(store, run_dir, deep=True)
        shutil.rmtree(store, ignore_errors=True)


def deterministic(campaign, command):
    """Run every deterministic case the harness's `command` runs, `scenario` or `retention`, under
    both builds, and the production build again on uring."""
    run_dir = campaign.run_dir(command)
    production = campaign.args.binary_na or campaign.args.binary
    passes = [("assertions", campaign.args.binary, "psync"), ("production", production, "psync"),
              ("production-uring", production, "uring")]
    for label, binary, io in passes:
        log = run_dir / f"{label}.log"
        # Scenarios that inject faults run a workload under the shim.
        campaign.run([str(binary), command, "all", "--scratch", str(campaign.scratch / f"{command}-{label}")], log,
                     env={"LORE_IO_BACKEND": io, "STRESS_SHIM": str(campaign.args.shim)}, timeout=7200)
        for line in log.read_text(errors="replace").splitlines():
            if line.startswith("FAIL "):
                name = line.split(":")[0][5:]
                campaign.record(run_dir, None, {f"{command} {name} ({label})": 1})
            if line.startswith("REPORT "):
                name, _, detail = line[7:].partition(": ")
                campaign.reports[f"{command} {name}"] = detail
            if line.startswith(("PASS ", "FAIL ", "REPORT ")):
                print(f"  {label}: {line[:220]}", flush=True)


def multi_process(campaign):
    """Rounds of processes sharing one store directory in calls, some keeping their stores between
    calls and some opening them for each, every one dropping operations and flushes unfinished,
    killed with SIGKILL and restarted at random moments or killed by the shim at a chosen file
    operation, some under the shim's faults on either store's files or with its file operations
    slowed. Each round is verified on a copy, a quarter of them deeply, and the next round carries
    on from what the last left."""
    rng = campaign.rng
    while not campaign.out_of_time():
        store = campaign.scratch / f"shared-{campaign.runs + 1:05d}"
        prepare = campaign.run_dir("shared-prepare")
        # Low fan-out thresholds, so that both stores' groups fan out through their levels.
        options = random_store_options(rng, "client") + ["--fan-out-threshold", rng.choice([2, 3, 8, 24])]
        # One process first, which fixes the keyspace and writes what a store's first opening does.
        campaign.run(campaign.command("calls", store, *options, "--seconds", 1), prepare / "prepare.log", timeout=600)
        for round_index in range(rng.randint(1, 3)):
            if campaign.out_of_time():
                break
            run_dir = campaign.run_dir(f"shared-r{round_index}")
            shared_round(campaign, store, run_dir)
            classes = campaign.verify(store, run_dir, deep=rng.random() < 0.25, copy=True)
            if classes and campaign.args.stop_on_new and any(known(name) is None for name in classes):
                return
        if not campaign.args.keep:
            shutil.rmtree(store, ignore_errors=True)


def calls_options(rng, keeps):
    """Options of one `calls` process."""
    options = [
        "--run-seed", rng.randrange(1, 1 << 30),
        "--tasks", rng.choice([1, 2, 4, 8]),
        "--call-ops", rng.choice([4, 16, 64]),
        "--pause-ms", rng.choice([0, 5, 50]),
        "--unflushed", rng.choice([0, 0.2, 0.5]),
        # A delayed flush that fires during the round, now and then.
        "--flush-delay", rng.choice([3600, 3600, 3600, 1]),
        "--hot-keys", rng.choice([4, 48, 512]),
        "--hot-share", rng.choice([0.0, 0.3, 0.8]),
        "--threads", rng.choice([2, 4, 8]),
        "--compact-max-size", rng.choice(["1", "1,256k,4m,64m", "64k"]),
        "--evict-max-capacity", rng.choice(["1", "1,4096,1m", "100"]),
        "--hang-seconds", 120,
        # Callers giving up on operations and on the flushes that end calls.
        "--cancel", rng.choice([0.01, 0.05, 0.1, 0.2]), "--cancel-us", rng.choice([50, 500, 5000, 50000]),
        "--audit-files", rng.choice([0, 0.25, 1]),
        "--spawn-flushes", rng.choice([0, 0.5]),
    ]
    mix = rng.choice(MIXES)
    if mix:
        options += ["--mix", mix]
    if rng.random() < 0.5:
        options.append("--serialize-flushes")
    if keeps:
        options.append("--keeps")
        if rng.random() < 0.5:
            # A keep-alive window, at times shorter than a slowed write.
            options += ["--keep-alive-ms", rng.choice([30, 300])]
    return options


def shared_round(campaign, store, run_dir):
    """Run `--processes` processes on `store` for a while, half of them keeping their stores, each
    killed now and then, and started again in its place once it is gone. Most run under the shim,
    whose state places each kill by the store operations running and the file operations in
    flight."""
    rng = campaign.rng
    seconds = rng.choice([5, 10, 20])
    deadline = time.time() + seconds
    slots = []
    runs = Counter()

    def start(slot):
        keeps = slot % 2 == 0
        remaining = max(0.5, deadline - time.time())
        runs[slot] += 1
        name = run_dir / f"p{slot}-{runs[slot]}"
        env = backend(rng, 0.1)
        if env["LORE_IO_BACKEND"] == "psync":
            env = watched(campaign, store, name.with_suffix(".state"), STRESS_OPS=",".join(ALL_OPS),
                          STRESS_PLACES=name.with_suffix(".places"))
            counted = sorted(key for key in campaign.events if key[0] == "seen" and eligible("kill", key[1], key[2]))
            roll = rng.random()
            if roll < 0.25:
                # Faults on the files of either store or both, each an error the store meets on its own.
                op = rng.choice(ALL_OPS)
                env.update({"STRESS_OPS": op, "STRESS_FAIL_ERRNO": rng.choice(NATURAL_ERRNOS[op]),
                            "STRESS_FAIL_PROB": str(rng.choice([0.001, 0.01])),
                            "STRESS_SEED": str(rng.randrange(1 << 30))})
                files = rng.choice(["immutable", "mutable", None])
                if files:
                    env["STRESS_PATH"] = str(store / files)
            elif roll < 0.4 and counted:
                # Killed at a file operation of a kind counted so far, in a store operation it ran in.
                _, operation, step = rng.choice(counted)
                op, file_class = step.split(":", 1)
                env.update(target_env(campaign, store, op, file_class, None if operation == "idle" else operation))
                env.update(crash_env(rng, op, rng.randint(1, 20), name.with_suffix(".killed"))[1])
            if rng.random() < 0.2:
                # A slow device: file operations held up, some for longer than a keep-alive window.
                micros, probability = rng.choice([(2000, 0.2), (20000, 0.02), (200000, 0.005)])
                env.update({"STRESS_DELAY_US": str(micros), "STRESS_DELAY_PROB": str(probability)})
        command = campaign.command("calls", store, *calls_options(rng, keeps), "--seconds", f"{remaining:.1f}")
        return campaign.start(command, name.with_suffix(".log"), env=env), name

    def reap(slot, killed):
        """Wait for the process in `slot`, mark it dead in the journal, which ends any generation
        it left unended, place where it was killed, take its log and placements, then start another
        in its place while the round lasts."""
        process, name = slots[slot]
        code = process.wait()
        with open(store / "stress.journal", "a") as journal:
            journal.write(f"0 g dead pid={process.pid}\n")
        crashed = campaign.note_kill(name.with_suffix(".killed"))
        if killed and name.with_suffix(".state").exists():
            campaign.note_state_kill(name.with_suffix(".state"))
        elif not killed and not crashed and code not in (0, 1):
            campaign.failures[f"calls-exit-{code}"] += 1
        campaign.place(read_places(name.with_suffix(".places")))
        campaign.parse_work(name.with_suffix(".log"), run_dir)
        slots[slot] = start(slot) if time.time() < deadline - 0.5 else None

    for slot in range(campaign.args.processes):
        slots.append(start(slot))
    while time.time() < deadline:
        time.sleep(rng.uniform(0.05, 0.5))
        for slot, entry in enumerate(slots):
            if entry is not None and entry[0].poll() is not None:
                reap(slot, False)
        slot = rng.randrange(len(slots))
        if rng.random() < 0.3 and slots[slot] is not None and slots[slot][0].poll() is None:
            slots[slot][0].send_signal(signal.SIGKILL)
            reap(slot, True)
    for slot, entry in enumerate(slots):
        if entry is None:
            continue
        try:
            entry[0].wait(timeout=180)
            reap(slot, False)
        except subprocess.TimeoutExpired:
            entry[0].send_signal(signal.SIGKILL)
            reap(slot, True)
            campaign.record(run_dir, store, {"calls-did-not-end": 1})


def build(args):
    shim = Path(args.shim)
    subprocess.run(["cc", "-O2", "-shared", "-fPIC", "-o", str(shim), str(HERE / "crashpoint.c"), "-ldl"], check=True)
    subprocess.run(["cargo", "build", "--release", "--example", "store_stress", "-p", "lore-storage", "--features", "store-stress"], cwd=REPO, check=True)
    env = dict(os.environ, CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS="false")
    subprocess.run(["cargo", "build", "--release", "--example", "store_stress", "-p", "lore-storage", "--features", "store-stress",
                    "--target-dir", str(REPO / "target" / "stress-production")], cwd=REPO, check=True, env=env)


MODES = {
    "kill-loop": kill_loop,
    "gc-storm": lambda campaign: kill_loop(campaign, "gc-storm"),
    "crash-sweep": crash_sweep,
    "fault-sweep": fault_sweep,
    "disk-full": disk_full,
    "emfile": descriptor_exhaustion,
    "scenarios": lambda campaign: deterministic(campaign, "scenario"),
    "retention": lambda campaign: deterministic(campaign, "retention"),
    "multi-process": multi_process,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=[*MODES, "all", "build", "report"])
    parser.add_argument("dir", nargs="?", help="for report: the results directory to merge")
    parser.add_argument("--minutes", type=float, default=10, help="time budget per mode")
    parser.add_argument("--results", default=str(REPO / "target" / "store-stress" / "results"))
    parser.add_argument("--scratch", default=str(REPO / "target" / "store-stress" / "scratch"))
    parser.add_argument("--binary", default=str(REPO / "target" / "stress-production" / "release" / "examples" / "store_stress"),
                        help="harness for workloads: by default the production build, without debug assertions")
    parser.add_argument("--binary-na", default=None, help="production build for scenarios, when --binary has assertions")
    parser.add_argument("--assertions-binary", default=str(REPO / "target" / "release" / "examples" / "store_stress"))
    parser.add_argument("--shim", default=str(REPO / "target" / "store-stress" / "crashpoint.so"))
    parser.add_argument("--image", default="ubuntu:26.04", help="container image for disk-full, with the host's libc")
    parser.add_argument("--seed", type=int, default=int(time.time()))
    parser.add_argument("--generations", type=int, default=6, help="most runs per store directory in a kill loop")
    parser.add_argument("--points", type=int, default=40, help="kill points per crash-sweep base, four times the "
                        "fault points per fault-sweep base")
    parser.add_argument("--processes", type=int, default=4, help="processes sharing a store in a multi-process round")
    parser.add_argument("--keep", action="store_true", help="keep scratch stores")
    parser.add_argument("--stop-on-new", action="store_true", help="stop at the first new violation class")
    args = parser.parse_args()
    Path(args.shim).parent.mkdir(parents=True, exist_ok=True)
    if args.mode == "build":
        build(args)
        return 0
    if args.mode == "report":
        return report(args.dir or args.results)
    modes = list(MODES) if args.mode == "all" else [args.mode]
    for mode in modes:
        if mode in ("scenarios", "retention"):
            # Deterministic cases run under both builds: assertions catch what production hides.
            scenario_args = argparse.Namespace(**vars(args))
            scenario_args.binary_na = args.binary
            scenario_args.binary = args.assertions_binary
            campaign = Campaign(scenario_args, mode)
        else:
            campaign = Campaign(args, mode)
        MODES[mode](campaign)
        campaign.summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
