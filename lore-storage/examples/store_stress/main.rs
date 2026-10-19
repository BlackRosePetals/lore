// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! A stress harness for the local immutable and mutable stores, run by hand rather than in CI. See
//! `scripts/store-stress/README.md` for how the orchestrator drives it.
//!
//! - `work <dir>` runs a randomized concurrent workload against the store in `<dir>`, journaling
//!   every operation it starts and every one that succeeds, so that a run killed at any point can
//!   be checked afterwards.
//! - `calls <dir>` runs the same workload, the mutable store's operations added, as one of several
//!   processes sharing `<dir>`, in calls as client processes make them: some keep their stores
//!   between calls and some open them for each. Each call holds what it reads to what the other
//!   processes' journaled operations settled.
//! - `verify <dir>` reopens the stores and checks them against the journal and against their own
//!   files.
//! - `scenario <name>|all|list` runs deterministic crash-window scenarios, each built from
//!   snapshots of a store directory taken at an exact point, which is what a process killed there
//!   leaves on disk.
//! - `retention <name>|all|list` runs deterministic interleavings of calls between store client
//!   processes on one directory, checking what each call found and how often it loaded each store.
//! - `client <dir>` is such a store client, serving requests on stdin.
//! - `info <dir>` prints the store's layout for forensics.
//! - `bench <dir>` measures the hot paths, puts, gets, reads into a caller's buffer and queries,
//!   so that a change to the store can be held to no regression.
//!
//! Every payload is derived from its key, and carries the key in its first bytes, so a payload
//! read back is checked byte for byte, and a wrong one names whose bytes it holds.

mod args;
mod bench;
mod calls;
mod client;
mod coverage;
mod journal;
mod model;
mod mutable;
mod online;
mod retention;
mod scenarios;
mod shim;
mod snapshot;
mod verify;
mod work;

use std::process::ExitCode;

fn usage() -> ExitCode {
    eprintln!(
        "usage: store_stress work <dir> [options] | calls <dir> [options] | verify <dir> [options] \
         | scenario <name|all|list> [--scratch <dir>] | retention <name|all|list> [--scratch <dir>] \
         | client <dir> [--keeps] | info <dir> | bench <dir>\n\
         see scripts/store-stress/README.md"
    );
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let args = args::Args::parse(std::env::args().skip(1));
    let Some(command) = args.positional.first().cloned() else {
        return usage();
    };
    if !matches!(
        command.as_str(),
        "work" | "calls" | "verify" | "scenario" | "retention" | "client" | "info" | "bench"
    ) {
        return usage();
    }
    // The scripts drive store clients, each a process of its own, and wait on nothing else.
    if command == "retention" {
        return ExitCode::from(retention::main(&args));
    }
    let threads = args.number("threads", 8) as usize;
    let runtime = match command.as_str() {
        // Scenarios and clients arm pause points, which are per thread, so they run on one.
        "scenario" | "client" => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build(),
        _ => tokio::runtime::Builder::new_multi_thread()
            .worker_threads(threads.max(1))
            .enable_all()
            .build(),
    };
    let runtime = match runtime {
        Ok(runtime) => runtime,
        Err(err) => {
            eprintln!("failed to build runtime: {err}");
            return ExitCode::from(2);
        }
    };
    let code = runtime.block_on(async {
        match command.as_str() {
            "work" => work::main(&args).await,
            "calls" => calls::main(&args).await,
            "verify" => verify::main(&args).await,
            "scenario" => scenarios::main(&args).await,
            "client" => client::main(&args).await,
            "info" => verify::info(&args),
            "bench" => bench::main(&args).await,
            _ => 2,
        }
    });
    ExitCode::from(code)
}
