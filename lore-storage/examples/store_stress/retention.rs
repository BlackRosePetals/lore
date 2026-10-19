// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Deterministic interleavings of calls between processes sharing a store directory. Each script
//! runs three store clients, see [`crate::client`], on a directory of its own: A and B keep their
//! stores between calls, reusing their state across holds of the stores' locks while the epochs are
//! unchanged, and C opens them afresh for each call. A script sends one request at a time and reads
//! its reply before it sends the next, so the interleaving is the script's and nothing waits on
//! time, and it checks each reply: how many times each store loaded, and what each operation found.
//! A script kills a client with SIGKILL in the middle of a call, with changes unflushed, or with a
//! fan-out commit half made, and has the others carry on.
//!
//! `retention <name>|all|list [--scratch <dir>] [--keep]` runs scripts, printing `PASS <name>` or
//! `FAIL <name>: <the step and what it found>` for each.

use std::collections::HashSet;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::ChildStdin;
use std::process::ChildStdout;
use std::process::Stdio;

use lore_storage::StoreMatch;

use crate::args::Args;
use crate::client::Op;
use crate::client::Outcome;
use crate::client::Outcome::Absent;
use crate::client::Outcome::Done;
use crate::client::Outcome::Found;
use crate::client::ran;
use crate::model::CONFIG_FILE;
use crate::model::Config;
use crate::mutable::KEYS;

type Script = fn(&mut Clients) -> Result<(), String>;

/// A script, with the keyspace of the store directory it runs on.
type Keyed = (fn() -> Config, Script);

/// A variant whose payloads are held locally, which eviction leaves.
const LOCAL: u32 = 0;
/// Another, in a partition of its own.
const OTHER: u32 = 1;
/// A variant whose payloads are durable, which eviction may take.
const DURABLE: u32 = 2;

/// The counters of the mutable keyspace.
const COUNTER: u64 = KEYS;
const SECOND_COUNTER: u64 = KEYS + 1;

macro_rules! script {
    ($function:ident) => {
        script!($function, keyspace)
    };
    ($function:ident, $keyspace:ident) => {
        (
            stringify!($function),
            ($keyspace as fn() -> Config, $function as Script),
        )
    };
}

fn scripts() -> Vec<(&'static str, Keyed)> {
    vec![
        script!(a_kept_store_reloads_once_to_see_a_put),
        script!(a_kept_store_sees_durable_and_fragment_list_puts),
        script!(a_kept_store_serves_nothing_another_process_obliterated),
        script!(a_kept_store_finds_a_copy_and_not_the_obliterated_source),
        script!(a_kept_store_serves_nothing_another_process_evicted),
        script!(a_kept_store_reads_and_writes_where_another_process_compacted),
        script!(a_kept_store_follows_a_group_fanned_out_through_every_level),
        script!(readers_leave_a_kept_store_current, real_hashes),
        script!(a_call_waits_for_another_process_to_flush),
        script!(a_call_waits_for_another_process_s_delayed_flush),
        script!(a_mutable_change_alone_keeps_both_stores_claimed),
        script!(a_writer_killed_with_unflushed_changes_leaves_nothing),
        script!(a_writer_killed_mid_call_leaves_what_it_flushed),
        script!(a_kept_store_rolls_forward_a_fan_out_a_killed_process_left_pending),
        script!(a_kept_store_rolls_forward_a_mutable_fan_out_a_killed_process_left_pending),
        script!(a_kept_store_loads_and_lists_what_another_process_stored),
        script!(a_kept_store_loads_what_another_process_swapped_in),
        script!(increments_from_every_process_add_up),
        script!(a_fresh_writer_invalidates_kept_stores),
        script!(a_kept_store_stays_current_through_its_own_writes),
        script!(a_dropped_flush_leaves_its_changes_to_the_next_flush),
        script!(a_dropped_flush_keeps_the_store_claimed_until_its_writes_end),
        script!(a_writer_killed_at_every_pause_point_leaves_a_consistent_store),
        script!(a_kept_store_follows_its_directory_deleted_and_created_again),
    ]
}

/// The keyspace of the scripts: every key in one group, which a fan-out threshold of 2 fans out
/// at three entries in a bucket, with fragment lists, and one partition of the three durable.
fn keyspace() -> Config {
    let mut config = crate::scenarios::config(2);
    config.partitions = 3;
    config.contexts = 1;
    config.evictable_partitions = 1;
    config.lists = true;
    config
}

/// [`keyspace`] with real hashes, which `verify` checks payloads against.
fn real_hashes() -> Config {
    let mut config = keyspace();
    config.real_hash = true;
    config
}

pub fn main(args: &Args) -> u8 {
    let name = args
        .positional
        .get(1)
        .cloned()
        .unwrap_or_else(|| "list".to_string());
    let selected = match crate::scenarios::select(&name, scripts()) {
        Ok(selected) => selected,
        Err(code) => return code,
    };
    let scratch = args.get("scratch").map_or_else(
        || std::env::temp_dir().join(format!("store-stress-retention-{}", std::process::id())),
        PathBuf::from,
    );
    let mut failed = 0;
    for (name, (keyspace, script)) in selected {
        match run(&scratch.join(name), keyspace(), script) {
            Ok(()) => println!("PASS {name}"),
            Err(detail) => {
                failed += 1;
                println!("FAIL {name}: {detail}");
            }
        }
    }
    if !args.flag("keep") {
        let _ = std::fs::remove_dir_all(&scratch);
    }
    u8::from(failed > 0)
}

/// Run `script` against clients A, B and C on a new store directory `dir` over `config`, then
/// hold the files they left to the store's structure, see [`crate::scenarios::check`].
fn run(
    dir: &Path,
    config: Config,
    script: impl FnOnce(&mut Clients) -> Result<(), String>,
) -> Result<(), String> {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir)
        .and_then(|()| std::fs::write(dir.join(CONFIG_FILE), config.serialize()))
        .map_err(|err| format!("prepare {}: {err}", dir.display()))?;
    let mut clients = Clients {
        a: Client::spawn(dir, "A", true)?,
        b: Client::spawn(dir, "B", true)?,
        c: Client::spawn(dir, "C", false)?,
        dir: dir.to_path_buf(),
        config: config.clone(),
    };
    script(&mut clients)?;
    for client in [clients.a, clients.b, clients.c] {
        client.finish()?;
    }
    let report = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| err.to_string())?
        .block_on(crate::scenarios::check(dir, &config, &[], &[]));
    match report.violation_count() {
        0 => Ok(()),
        count => {
            report.print();
            Err(format!(
                "the store files hold {count} violations, printed above"
            ))
        }
    }
}

struct Clients {
    dir: PathBuf,
    config: Config,
    a: Client,
    b: Client,
    c: Client,
}

impl Clients {
    /// `count` plain keys, none in `taken`, which they join.
    fn keys(&self, count: usize, taken: &mut HashSet<u64>) -> Vec<u64> {
        crate::scenarios::keys(&self.config, 0..255, 16..1 << 20, count, taken)
    }

    /// A key that is a fragment list, and its children.
    fn list(&self) -> (u64, Vec<u64>) {
        let list = (1..self.config.keys)
            .find(|key| self.config.is_list(*key))
            .unwrap_or_default();
        (list, self.config.children(list))
    }
}

/// A store client, as a script drives it.
struct Client {
    name: &'static str,
    keeps: bool,
    dir: PathBuf,
    process: Child,
    /// The client's stdin, closed to end its requests.
    requests: Option<ChildStdin>,
    replies: BufReader<ChildStdout>,
}

impl Client {
    /// Start store client `name` on the store directory `dir`, keeping its stores between calls if
    /// `keeps`.
    fn spawn(dir: &Path, name: &'static str, keeps: bool) -> Result<Client, String> {
        let exe = std::env::current_exe().map_err(|err| err.to_string())?;
        let mut command = std::process::Command::new(exe);
        command.arg("client").arg(dir);
        if keeps {
            command.arg("--keeps");
        }
        let mut process = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|err| format!("spawn client {name}: {err}"))?;
        let requests = process.stdin.take();
        let replies = process
            .stdout
            .take()
            .map(BufReader::new)
            .ok_or_else(|| format!("client {name} has no stdout"))?;
        Ok(Client {
            name,
            keeps,
            dir: dir.to_path_buf(),
            process,
            requests,
            replies,
        })
    }

    fn send(&mut self, request: &str, ops: &[Op]) -> Result<(), String> {
        let mut line = request.to_string();
        for op in ops {
            line.push_str(&format!(" {op}"));
        }
        line.push('\n');
        self.requests
            .as_mut()
            .ok_or_else(|| format!("client {} takes no more requests", self.name))?
            .write_all(line.as_bytes())
            .map_err(|err| format!("send client {} {request}: {err}", self.name))
    }

    /// The next reply, passing over anything else the client writes, as the store's own output.
    fn reply(&mut self) -> Result<String, String> {
        loop {
            let mut line = String::new();
            let read = self
                .replies
                .read_line(&mut line)
                .map_err(|err| format!("read client {}: {err}", self.name))?;
            if read == 0 {
                let status = self.process.wait().map_err(|err| err.to_string())?;
                return Err(format!(
                    "client {} exited with {status} instead of answering",
                    self.name
                ));
            }
            let line = line.trim_end();
            if ["ran ", "probed ", "paused", "unreached"]
                .iter()
                .any(|reply| line.starts_with(reply))
            {
                return Ok(line.to_string());
            }
            eprintln!("client {}: {line}", self.name);
        }
    }

    /// Check that the next reply is `expected`.
    fn expect_reply(&mut self, step: &str, expected: &str) -> Result<(), String> {
        let reply = self.reply()?;
        if reply == expected {
            return Ok(());
        }
        Err(format!(
            "{} {step}: expected `{expected}`, answered `{reply}`",
            self.name
        ))
    }

    /// Check that the reply to the last request names `loads` and `outcomes`.
    fn expect(
        &mut self,
        step: &str,
        loads: [usize; 2],
        outcomes: &[Outcome],
    ) -> Result<(), String> {
        self.expect_reply(step, &ran(loads, outcomes))
    }

    /// Run a call of `ops`, and check that it loaded the immutable and the mutable store `loads`
    /// times and had `outcomes`.
    fn call(
        &mut self,
        step: &str,
        ops: &[Op],
        loads: [usize; 2],
        outcomes: &[Outcome],
    ) -> Result<(), String> {
        self.send("call", ops)?;
        self.expect(step, loads, outcomes)
    }

    /// As [`Self::call`], for a call that does not flush the stores.
    fn unflushed(
        &mut self,
        step: &str,
        ops: &[Op],
        loads: [usize; 2],
        outcomes: &[Outcome],
    ) -> Result<(), String> {
        self.send("unflushed", ops)?;
        self.expect(step, loads, outcomes)
    }

    /// Start a call of `ops`, and check that another process held each store's flock as `busy`
    /// says when it began. [`Self::expect`] checks its reply.
    fn probed(&mut self, step: &str, ops: &[Op], busy: [bool; 2]) -> Result<(), String> {
        self.send("probed", ops)?;
        self.expect_reply(step, &format!("probed {} {}", busy[0], busy[1]))
    }

    /// As [`Self::call`], for a call that stays open after its operations, until the client is
    /// killed.
    fn hold(
        &mut self,
        step: &str,
        ops: &[Op],
        loads: [usize; 2],
        outcomes: &[Outcome],
    ) -> Result<(), String> {
        self.send("hold", ops)?;
        self.expect(step, loads, outcomes)
    }

    /// Run a call of `ops` whose operations and flush stop at the pause point `point`, as the
    /// client names it, until the client is killed.
    fn pause(&mut self, step: &str, point: &str, ops: &[Op]) -> Result<(), String> {
        self.send(&format!("pause {point}"), ops)?;
        self.expect_reply(step, "paused")
    }

    /// As [`Self::call`], for a call whose flush is dropped at the pause point `point`.
    fn cancel(
        &mut self,
        step: &str,
        point: &str,
        ops: &[Op],
        loads: [usize; 2],
        outcomes: &[Outcome],
    ) -> Result<(), String> {
        self.send(&format!("cancel {point}"), ops)?;
        self.expect(step, loads, outcomes)
    }

    /// Run a call of `ops`, and return what each found, as the reply words them, once each store
    /// loaded at most once.
    fn read(&mut self, step: &str, ops: &[Op]) -> Result<Vec<String>, String> {
        self.send("call", ops)?;
        let reply = self.reply()?;
        let words: Vec<&str> = reply.split_whitespace().collect();
        let loads_once = words.get(1..3).is_some_and(|loads| {
            loads
                .iter()
                .all(|count| count.parse::<usize>().is_ok_and(|count| count <= 1))
        });
        if words.first() != Some(&"ran") || !loads_once || words.len() != 3 + ops.len() {
            return Err(format!("{} {step}: answered `{reply}`", self.name));
        }
        Ok(words[3..].iter().map(|word| (*word).to_string()).collect())
    }

    /// Flush the stores outside any call, and check that neither loaded.
    fn flush(&mut self, step: &str) -> Result<(), String> {
        self.send("flush", &[])?;
        self.expect(step, [0, 0], &[Done, Done])
    }

    /// Run the delayed flushes that are due outside any call, and check that each store had `due`
    /// of them and that neither loaded.
    fn sweep(&mut self, step: &str, due: [usize; 2]) -> Result<(), String> {
        self.send("sweep", &[])?;
        self.expect(step, [0, 0], &due.map(Outcome::Swept))
    }

    /// Kill the client with SIGKILL, and start another in its place.
    fn kill_and_restart(&mut self) -> Result<(), String> {
        self.process
            .kill()
            .and_then(|()| self.process.wait().map(drop))
            .map_err(|err| format!("kill client {}: {err}", self.name))?;
        *self = Client::spawn(&self.dir, self.name, self.keeps)?;
        Ok(())
    }

    /// End the client's requests, and check that it exits as it should.
    fn finish(mut self) -> Result<(), String> {
        drop(self.requests.take());
        let status = self.process.wait().map_err(|err| err.to_string())?;
        match status.success() {
            true => Ok(()),
            false => Err(format!("client {} exited with {status}", self.name)),
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn put(key: u64) -> Op {
    Op::Put(key, LOCAL)
}

fn get(key: u64) -> Op {
    Op::Get(key, LOCAL)
}

fn get_into(key: u64) -> Op {
    Op::GetInto(key, LOCAL)
}

fn query(key: u64) -> Op {
    Op::Query(key, LOCAL)
}

fn metadata(key: u64) -> Op {
    Op::Metadata(key, LOCAL)
}

/// Found by a lookup, under its own association, in the partition of `variant`.
fn found_in(variant: u32) -> Outcome {
    Outcome::Matched(StoreMatch::MatchFull, variant as u8)
}

/// Found by a lookup in another partition than the one asked about, that of `variant`.
fn matched_elsewhere(variant: u32) -> Outcome {
    Outcome::Matched(StoreMatch::MatchHash, variant as u8)
}

/// **A kept store reloads once to see what another process put, and not again after, and readers
/// leave the writer's kept store current.**
fn a_kept_store_reloads_once_to_see_a_put(clients: &mut Clients) -> Result<(), String> {
    let x = clients.keys(1, &mut HashSet::new())[0];
    let Clients { a, b, c, .. } = clients;
    let reads = [get(x), get_into(x), query(x), metadata(x)];
    let found = [Found, Found, found_in(LOCAL), found_in(LOCAL)];
    b.call("opens its stores", &[get(x)], [1, 1], &[Absent])?;
    a.call("puts x", &[put(x)], [1, 1], &[Done])?;
    b.call("reads x after A put it", &reads, [1, 0], &found)?;
    b.call("reads x again", &reads, [0, 0], &found)?;
    c.call("reads x", &reads, [1, 1], &found)?;
    a.call("reads x after B and C read it", &[get(x)], [0, 0], &[Found])
}

/// **A kept store reloads to see a durable payload and a fragment list another process put.**
fn a_kept_store_sees_durable_and_fragment_list_puts(clients: &mut Clients) -> Result<(), String> {
    let y = clients.keys(1, &mut HashSet::new())[0];
    let (list, children) = clients.list();
    let Clients { a, b, c, .. } = clients;
    let mut reads = vec![Op::Get(y, DURABLE), get(list), query(list)];
    reads.extend(children.iter().map(|child| get(*child)));
    let mut found = vec![Found, Found, found_in(LOCAL)];
    found.extend(children.iter().map(|_| Found));
    let absent = [Absent, Absent];
    b.call("opens its stores", &reads[..2], [1, 1], &absent)?;
    a.call(
        "puts y durably and the list after its children",
        &[Op::Put(y, DURABLE), put(list)],
        [1, 1],
        &[Done, Done],
    )?;
    b.call("reads y, the list and its children", &reads, [1, 0], &found)?;
    c.call("reads y, the list and its children", &reads, [1, 1], &found)
}

/// **A kept store serves nothing another process obliterated, a fragment list's children
/// included.**
fn a_kept_store_serves_nothing_another_process_obliterated(
    clients: &mut Clients,
) -> Result<(), String> {
    let x = clients.keys(1, &mut HashSet::new())[0];
    let (list, children) = clients.list();
    let Clients { a, b, c, .. } = clients;
    let mut reads = vec![get(x), query(x), get(list)];
    reads.extend(children.iter().map(|child| get(*child)));
    let mut found = vec![Found, found_in(LOCAL), Found];
    found.extend(children.iter().map(|_| Found));
    let absent = vec![Absent; reads.len()];
    a.call(
        "puts x and the list",
        &[put(x), put(list)],
        [1, 1],
        &[Done, Done],
    )?;
    b.call("reads them", &reads, [1, 1], &found)?;
    a.call(
        "obliterates x and the list",
        &[Op::Obliterate(x, LOCAL), Op::Obliterate(list, LOCAL)],
        [0, 0],
        &[Done, Done],
    )?;
    b.call(
        "reads them after A obliterated them",
        &reads,
        [1, 0],
        &absent,
    )?;
    c.call("reads them", &reads, [1, 1], &absent)
}

/// **A kept store finds a payload where another process copied it, and not where it obliterated
/// it.**
fn a_kept_store_finds_a_copy_and_not_the_obliterated_source(
    clients: &mut Clients,
) -> Result<(), String> {
    let x = clients.keys(1, &mut HashSet::new())[0];
    let Clients { a, b, c, .. } = clients;
    let looks = [
        query(x),
        Op::Query(x, OTHER),
        metadata(x),
        Op::Metadata(x, OTHER),
        Op::Get(x, OTHER),
    ];
    let found = [Absent, found_in(OTHER), Absent, found_in(OTHER), Found];
    a.call("puts x", &[put(x)], [1, 1], &[Done])?;
    b.call(
        "finds x in the first partition",
        &[query(x), metadata(x)],
        [1, 1],
        &[found_in(LOCAL), found_in(LOCAL)],
    )?;
    a.call(
        "copies x to another partition and obliterates it in the first",
        &[Op::Copy(x, LOCAL, OTHER), Op::Obliterate(x, LOCAL)],
        [0, 0],
        &[Done, Done],
    )?;
    b.call(
        "finds x in the other partition alone",
        &looks,
        [1, 0],
        &found,
    )?;
    c.call(
        "finds x in the other partition alone",
        &looks,
        [1, 1],
        &found,
    )
}

/// **A kept store serves no payload another process evicted, and the payloads eviction leaves.**
fn a_kept_store_serves_nothing_another_process_evicted(
    clients: &mut Clients,
) -> Result<(), String> {
    let [x, y, z] = clients.keys(3, &mut HashSet::new())[..] else {
        return Err("three keys".to_string());
    };
    let Clients { a, b, c, .. } = clients;
    let reads = [Op::Get(x, DURABLE), Op::Get(y, DURABLE), get(z)];
    a.call(
        "puts x and y durably and z locally",
        &[Op::Put(x, DURABLE), Op::Put(y, DURABLE), put(z)],
        [1, 1],
        &[Done, Done, Done],
    )?;
    b.call("reads x, y and z", &reads, [1, 1], &[Found, Found, Found])?;
    a.call("evicts", &[Op::Evict], [0, 0], &[Outcome::Evicted(2)])?;
    b.call(
        "reads x, y and z after A evicted",
        &reads,
        [1, 0],
        &[Absent, Absent, Found],
    )?;
    c.call("reads x, y and z", &reads, [1, 1], &[Absent, Absent, Found])
}

/// **A kept store reads a payload where another process's compaction moved it, and writes to the
/// packfiles the compaction left.**
fn a_kept_store_reads_and_writes_where_another_process_compacted(
    clients: &mut Clients,
) -> Result<(), String> {
    let [y, x, z] = clients.keys(3, &mut HashSet::new())[..] else {
        return Err("three keys".to_string());
    };
    let Clients { a, b, c, .. } = clients;
    let reads = [
        get(x),
        get_into(x),
        Op::Get(y, DURABLE),
        get(z),
        Op::LoadIndex,
    ];
    a.call(
        "puts y durably and then x, in one packfile",
        &[Op::Put(y, DURABLE), put(x)],
        [1, 1],
        &[Done, Done],
    )?;
    b.call("reads x", &[get(x)], [1, 1], &[Found])?;
    a.call("compacts, evicting y", &[Op::Compact], [0, 0], &[Done])?;
    b.call(
        "reads where A compacted, and puts z",
        &[get(x), get_into(x), Op::Get(y, DURABLE), put(z)],
        [1, 0],
        &[Found, Found, Absent, Done],
    )?;
    let found = [Found, Found, Absent, Found, Done];
    a.call(
        "reads what B put after the compaction",
        &reads,
        [1, 0],
        &found,
    )?;
    c.call("reads x, y and z", &reads, [1, 1], &found)
}

/// **A kept store follows a group another process fans out through every level of the ladder.**
/// Three keys in one bucket are over the threshold of 2, and each key after them lands in the
/// bucket that holds them at the level the group reached.
fn a_kept_store_follows_a_group_fanned_out_through_every_level(
    clients: &mut Clients,
) -> Result<(), String> {
    let mut taken = HashSet::new();
    let mut at = |byte: u8, count: usize| {
        crate::scenarios::keys(
            &clients.config,
            byte..byte + 1,
            16..1 << 20,
            count,
            &mut taken,
        )
    };
    let mut keys = at(0, 3);
    let steps = [(at(4, 1)[0], 64), (at(2, 1)[0], 128), (at(1, 1)[0], 256)];
    let Clients { a, b, c, .. } = clients;
    let reads = |keys: &[u64]| -> Vec<Op> {
        keys.iter()
            .map(|key| get(*key))
            .chain([Op::Level(keys[0])])
            .collect()
    };
    let found = |keys: &[u64], level: usize| -> Vec<Outcome> {
        keys.iter()
            .map(|_| Found)
            .chain([Outcome::Level(level)])
            .collect()
    };
    a.call("puts the first key", &[put(keys[0])], [1, 1], &[Done])?;
    b.call(
        "reads it in a group of one bucket",
        &reads(&keys[..1]),
        [1, 1],
        &found(&keys[..1], 1),
    )?;
    a.call(
        "puts two more in its bucket",
        &[put(keys[1]), put(keys[2])],
        [0, 0],
        &[Done, Done],
    )?;
    b.call(
        "reads the group at 32 buckets",
        &reads(&keys),
        [1, 0],
        &found(&keys, 32),
    )?;
    for (key, level) in steps {
        keys.push(key);
        a.call(
            "puts a key in the full bucket",
            &[put(key)],
            [0, 0],
            &[Done],
        )?;
        b.call(
            &format!("reads the group at {level} buckets"),
            &reads(&keys),
            [1, 0],
            &found(&keys, level),
        )?;
    }
    c.call("reads the group", &reads(&keys), [1, 1], &found(&keys, 256))
}

/// **Reads, index loads, verification, loads and lists leave another process's kept stores
/// current.**
fn readers_leave_a_kept_store_current(clients: &mut Clients) -> Result<(), String> {
    let x = clients.keys(1, &mut HashSet::new())[0];
    let Clients { a, b, c, .. } = clients;
    let reads = [
        get(x),
        get_into(x),
        query(x),
        metadata(x),
        Op::LoadIndex,
        Op::Verify,
        Op::Load(0),
        Op::List,
    ];
    let found = [
        Found,
        Found,
        found_in(LOCAL),
        found_in(LOCAL),
        Done,
        Done,
        Outcome::Value(7),
        Outcome::Listed(vec![(0, 7)]),
    ];
    a.call(
        "puts x and stores 7 for k",
        &[put(x), Op::Store(0, 7)],
        [1, 1],
        &[Done, Done],
    )?;
    b.call("reads every way", &reads, [1, 1], &found)?;
    c.call("reads every way", &reads, [1, 1], &found)?;
    a.call(
        "reads after B and C read",
        &[get(x), Op::Load(0)],
        [0, 0],
        &[Found, Outcome::Value(7)],
    )?;
    b.call("reads every way again", &reads, [0, 0], &found)
}

/// **Another process's unflushed changes keep its claims until it flushes**, so a call waits for
/// the flush and then sees the changes.
fn a_call_waits_for_another_process_to_flush(clients: &mut Clients) -> Result<(), String> {
    let x = clients.keys(1, &mut HashSet::new())[0];
    let Clients { a, b, c, .. } = clients;
    let reads = [get(x), Op::Load(0)];
    let found = [Found, Outcome::Value(1)];
    b.call("opens its stores", &reads, [1, 1], &[Absent, Absent])?;
    a.unflushed(
        "puts x and stores 1 for k without flushing",
        &[put(x), Op::Store(0, 1)],
        [1, 1],
        &[Done, Done],
    )?;
    b.probed("starts reading x and k", &reads, [true, true])?;
    a.flush("flushes")?;
    b.expect("reads x and k once A flushed", [1, 1], &found)?;
    c.call("reads x and k", &reads, [1, 1], &found)
}

/// **Another process's unflushed changes keep its claims until its delayed flushes write them**,
/// after its call ended, so a call waits for them and then sees the changes.
fn a_call_waits_for_another_process_s_delayed_flush(clients: &mut Clients) -> Result<(), String> {
    let x = clients.keys(1, &mut HashSet::new())[0];
    let Clients { a, b, c, .. } = clients;
    let reads = [get(x), Op::Load(0)];
    let found = [Found, Outcome::Value(1)];
    b.call("opens its stores", &reads, [1, 1], &[Absent, Absent])?;
    a.unflushed(
        "puts x and stores 1 for k without flushing",
        &[put(x), Op::Store(0, 1)],
        [1, 1],
        &[Done, Done],
    )?;
    b.probed("starts reading x and k", &reads, [true, true])?;
    a.sweep("runs the delayed flush each store has due", [1, 1])?;
    b.expect("reads x and k once A's delayed flushes ran", [1, 1], &found)?;
    c.call("reads x and k", &reads, [1, 1], &found)
}

/// **Another process's unflushed change to the mutable store alone keeps its claims on both
/// stores**, so a call opening the stores waits for the immutable store, which every call claims
/// first. Holding the immutable store while it waited for the mutable one, it would keep the other
/// process's next call from claiming the immutable store, and neither would go on.
fn a_mutable_change_alone_keeps_both_stores_claimed(clients: &mut Clients) -> Result<(), String> {
    let Clients { a, c, .. } = clients;
    a.unflushed(
        "stores 1 for k without flushing",
        &[Op::Store(0, 1)],
        [1, 1],
        &[Done],
    )?;
    c.probed("starts loading k", &[Op::Load(0)], [true, true])?;
    a.call("stores 2 for k", &[Op::Store(0, 2)], [0, 0], &[Done])?;
    c.expect("loads k once A flushed", [1, 1], &[Outcome::Value(2)])
}

/// **A writer killed with changes unflushed leaves none of them, and the readers' kept stores
/// reload what it left.** Its writes advanced the epochs before they changed anything.
fn a_writer_killed_with_unflushed_changes_leaves_nothing(
    clients: &mut Clients,
) -> Result<(), String> {
    let [x, y] = clients.keys(2, &mut HashSet::new())[..] else {
        return Err("two keys".to_string());
    };
    let Clients { a, b, c, .. } = clients;
    let reads = [get(x), get(y), Op::Query(x, OTHER), Op::Load(0)];
    let before = [Found, Absent, matched_elsewhere(LOCAL), Outcome::Value(1)];
    a.call(
        "puts x and stores 1 for k",
        &[put(x), Op::Store(0, 1)],
        [1, 1],
        &[Done, Done],
    )?;
    b.call("reads x, y and k", &reads, [1, 1], &before)?;
    a.unflushed(
        "puts y, copies x and stores 2 for k, without flushing",
        &[put(y), Op::Copy(x, LOCAL, OTHER), Op::Store(0, 2)],
        [0, 0],
        &[Done, Done, Done],
    )?;
    a.kill_and_restart()?;
    b.call("reads after A was killed", &reads, [1, 1], &before)?;
    c.call("reads x, y and k", &reads, [1, 1], &before)?;
    a.call("reads x, y and k, restarted", &reads, [1, 1], &before)
}

/// **A writer killed in the middle of a call leaves what it flushed in the call and the
/// obliteration it made, which writes its tombstone as it runs, and nothing else after.**
fn a_writer_killed_mid_call_leaves_what_it_flushed(clients: &mut Clients) -> Result<(), String> {
    let [x, z] = clients.keys(2, &mut HashSet::new())[..] else {
        return Err("two keys".to_string());
    };
    let Clients { a, b, c, .. } = clients;
    let reads = [get(x), get(z), Op::Load(0)];
    let after = [Found, Absent, Outcome::Value(1)];
    b.call(
        "opens its stores",
        &reads,
        [1, 1],
        &[Absent, Absent, Absent],
    )?;
    a.hold(
        "puts x and z and stores 1 for k, flushes, then stores 2 for k and obliterates z",
        &[
            put(x),
            put(z),
            Op::Store(0, 1),
            Op::Flush,
            Op::Store(0, 2),
            Op::Obliterate(z, LOCAL),
        ],
        [1, 1],
        &[Done, Done, Done, Done, Done, Done],
    )?;
    a.kill_and_restart()?;
    b.call("reads after A was killed", &reads, [1, 1], &after)?;
    c.call("reads x, y and k", &reads, [1, 1], &after)
}

/// **A kept store finds the entries of a fan-out commit a killed process left pending**, which
/// its reload rolls forward.
fn a_kept_store_rolls_forward_a_fan_out_a_killed_process_left_pending(
    clients: &mut Clients,
) -> Result<(), String> {
    let [w, x, y] = clients.keys(3, &mut HashSet::new())[..] else {
        return Err("three keys".to_string());
    };
    let pending = crate::scenarios::group_dir(&clients.dir).join("level.pending");
    let Clients { a, b, c, .. } = clients;
    let reads = [get(w), get(x), get(y), Op::Level(w)];
    let found = [Found, Found, Found, Outcome::Level(32)];
    a.call("puts w", &[put(w)], [1, 1], &[Done])?;
    b.call(
        "reads w in a group of one bucket",
        &[get(w), Op::Level(w)],
        [1, 1],
        &[Found, Outcome::Level(1)],
    )?;
    a.pause(
        "puts x and y, fanning the group out, and stops in the commit",
        "committing",
        &[put(x), put(y)],
    )?;
    if !pending.exists() {
        return Err("the stopped commit left no level.pending".to_string());
    }
    a.kill_and_restart()?;
    b.call("reads the group after A was killed", &reads, [1, 0], &found)?;
    if pending.exists() {
        return Err("B's reload left level.pending in place".to_string());
    }
    c.call("reads the group", &reads, [1, 1], &found)
}

/// **A kept mutable store finds the values of a fan-out commit a killed process left pending.**
fn a_kept_store_rolls_forward_a_mutable_fan_out_a_killed_process_left_pending(
    clients: &mut Clients,
) -> Result<(), String> {
    let Clients { a, b, c, .. } = clients;
    // Keys 0, 2 and 4 share a group and a bucket.
    let reads = [Op::Load(0), Op::Load(2), Op::Load(4), Op::List];
    let found = [
        Outcome::Value(1),
        Outcome::Value(2),
        Outcome::Value(3),
        Outcome::Listed(vec![(0, 1), (2, 2), (4, 3)]),
    ];
    a.call("stores 1 for key 0", &[Op::Store(0, 1)], [1, 1], &[Done])?;
    b.call(
        "loads and lists key 0",
        &[Op::Load(0), Op::List],
        [1, 1],
        &[Outcome::Value(1), Outcome::Listed(vec![(0, 1)])],
    )?;
    a.pause(
        "stores keys 2 and 4, fanning their group out, and stops in the commit",
        "committing",
        &[Op::Store(2, 2), Op::Store(4, 3)],
    )?;
    a.kill_and_restart()?;
    b.call("loads and lists after A was killed", &reads, [0, 1], &found)?;
    c.call("loads and lists", &reads, [1, 1], &found)
}

/// **A kept store loads and lists the values another process stored, over a group it fanned
/// out, reloading once.**
fn a_kept_store_loads_and_lists_what_another_process_stored(
    clients: &mut Clients,
) -> Result<(), String> {
    let Clients { a, b, c, .. } = clients;
    let reads = [Op::Load(0), Op::Load(1), Op::Load(2), Op::Load(4), Op::List];
    let found = [
        Outcome::Value(5),
        Outcome::Value(6),
        Outcome::Value(7),
        Outcome::Value(8),
        Outcome::Listed(vec![(0, 5), (1, 6), (2, 7), (4, 8)]),
    ];
    b.call(
        "opens its stores",
        &[Op::Load(0), Op::List],
        [1, 1],
        &[Absent, Outcome::Listed(vec![])],
    )?;
    a.call(
        "stores four keys",
        &[
            Op::Store(0, 5),
            Op::Store(1, 6),
            Op::Store(2, 7),
            Op::Store(4, 8),
        ],
        [1, 1],
        &[Done, Done, Done, Done],
    )?;
    b.call("loads and lists them", &reads, [0, 1], &found)?;
    b.call("loads and lists them again", &reads, [0, 0], &found)?;
    c.call("loads and lists them", &reads, [1, 1], &found)
}

/// **A kept store loads the value another process swapped in, and a swap from the value it held
/// before fails.**
fn a_kept_store_loads_what_another_process_swapped_in(clients: &mut Clients) -> Result<(), String> {
    let Clients { a, b, c, .. } = clients;
    let load = Op::Load(COUNTER);
    a.call("stores 1", &[Op::Store(COUNTER, 1)], [1, 1], &[Done])?;
    b.call("loads it", &[load], [1, 1], &[Outcome::Value(1)])?;
    a.call("swaps 1 for 2", &[Op::Swap(COUNTER, 1, 2)], [0, 0], &[Done])?;
    b.call(
        "loads, and swaps from the 1 it held before",
        &[load, Op::Swap(COUNTER, 1, 3), load],
        [0, 1],
        &[Outcome::Value(2), Outcome::Refused(2), Outcome::Value(2)],
    )?;
    c.call("loads", &[load], [1, 1], &[Outcome::Value(2)])
}

/// **Increments by compare-and-swap from every process add up, and one from a value another
/// process moved past is refused.**
fn increments_from_every_process_add_up(clients: &mut Clients) -> Result<(), String> {
    let Clients { a, b, c, .. } = clients;
    let load = Op::Load(SECOND_COUNTER);
    let swap = |from, to| Op::Swap(SECOND_COUNTER, from, to);
    a.call("increments from 0", &[swap(0, 1)], [1, 1], &[Done])?;
    b.call(
        "loads and increments",
        &[load, swap(1, 2)],
        [1, 1],
        &[Outcome::Value(1), Done],
    )?;
    c.call(
        "loads and increments",
        &[load, swap(2, 3)],
        [1, 1],
        &[Outcome::Value(2), Done],
    )?;
    a.call(
        "increments from the 1 it last saw",
        &[swap(1, 2)],
        [0, 1],
        &[Outcome::Refused(3)],
    )?;
    a.call("increments from 3", &[swap(3, 4)], [0, 0], &[Done])?;
    b.call("loads", &[load], [0, 1], &[Outcome::Value(4)])?;
    c.call("loads", &[load], [1, 1], &[Outcome::Value(4)])
}

/// **A process that opens its stores afresh for each call invalidates kept stores as a keeping
/// one does.**
fn a_fresh_writer_invalidates_kept_stores(clients: &mut Clients) -> Result<(), String> {
    let x = clients.keys(1, &mut HashSet::new())[0];
    let Clients { a, b, c, .. } = clients;
    let reads = [get(x), Op::Load(3)];
    let found = [Found, Outcome::Value(9)];
    for kept in [&mut *a, &mut *b] {
        kept.call("opens its stores", &reads, [1, 1], &[Absent, Absent])?;
    }
    c.call(
        "puts x and stores 9 for k",
        &[put(x), Op::Store(3, 9)],
        [1, 1],
        &[Done, Done],
    )?;
    for kept in [&mut *a, &mut *b] {
        kept.call("reads what C wrote", &reads, [1, 1], &found)?;
    }
    c.call(
        "obliterates x",
        &[Op::Obliterate(x, LOCAL)],
        [1, 1],
        &[Done],
    )?;
    for kept in [&mut *a, &mut *b] {
        kept.call("reads after C obliterated x", &[get(x)], [1, 0], &[Absent])?;
    }
    Ok(())
}

/// **A kept store stays current through its own writes, which the other processes then see.**
fn a_kept_store_stays_current_through_its_own_writes(clients: &mut Clients) -> Result<(), String> {
    let [x, y] = clients.keys(2, &mut HashSet::new())[..] else {
        return Err("two keys".to_string());
    };
    let Clients { a, b, c, .. } = clients;
    let reads = [get(x), get(y), Op::Load(5)];
    let found = [Found, Found, Outcome::Value(2)];
    a.call(
        "puts x and stores 1 for k",
        &[put(x), Op::Store(5, 1)],
        [1, 1],
        &[Done, Done],
    )?;
    a.call(
        "reads them, puts y and swaps k from 1 to 2",
        &[get(x), Op::Load(5), put(y), Op::Swap(5, 1, 2)],
        [0, 0],
        &[Found, Outcome::Value(1), Done, Done],
    )?;
    a.call("reads them all", &reads, [0, 0], &found)?;
    b.call("reads them all", &reads, [1, 1], &found)?;
    c.call("reads them all", &reads, [1, 1], &found)
}

/// **A flush its caller dropped part way leaves what it did not write to the next flush, and
/// keeps the claims on both stores until then.** A's flush is dropped at the immutable store's
/// first bucket write, which the store runs on, so the mutable store's change stays unwritten.
fn a_dropped_flush_leaves_its_changes_to_the_next_flush(
    clients: &mut Clients,
) -> Result<(), String> {
    let x = clients.keys(1, &mut HashSet::new())[0];
    let Clients { a, b, c, .. } = clients;
    let reads = [get(x), Op::Load(0)];
    let flushed = [Found, Outcome::Value(1)];
    b.call("opens its stores", &reads, [1, 1], &[Absent, Absent])?;
    a.cancel(
        "puts x and stores 1 for key 0, its flush dropped at the first bucket write",
        "serializing",
        &[put(x), Op::Store(0, 1)],
        [1, 1],
        &[Done, Done],
    )?;
    b.probed("starts reading", &reads, [true, true])?;
    a.flush("flushes")?;
    b.expect("reads once A flushed", [1, 1], &flushed)?;
    c.call("reads", &reads, [1, 1], &flushed)
}

/// **A writer killed at any pause point of a call leaves a store every process reads alike**:
/// what earlier calls flushed, and of the killed call's changes all or some, never a damaged
/// payload. Each case runs on a directory of its own: A puts and stores a baseline, B reads it,
/// A runs the case's call until it reaches the pause point and is killed there, and then B, C
/// and A restarted read the same, the baseline unchanged where the call did not change it.
fn a_writer_killed_at_every_pause_point_leaves_a_consistent_store(
    clients: &mut Clients,
) -> Result<(), String> {
    let [x, y, z, d] = clients.keys(4, &mut HashSet::new())[..] else {
        return Err("four keys".to_string());
    };
    let reads = [
        get(x),
        Op::Get(d, DURABLE),
        get(y),
        get(z),
        Op::Level(x),
        Op::Load(0),
        Op::Load(2),
        Op::Load(4),
    ];
    // Each case: the pause point, the call reaching it, and the reads the call may change.
    let cases: [(&str, Vec<Op>, &[usize]); 18] = [
        ("advanced", vec![put(y)], &[2]),
        ("reserved", vec![put(y)], &[2]),
        ("written", vec![put(y)], &[2]),
        ("scanning", vec![put(y)], &[2]),
        ("serializing", vec![put(y)], &[2]),
        ("committing", vec![put(y), put(z)], &[2, 3, 4]),
        ("serializing", vec![Op::Obliterate(x, LOCAL)], &[0]),
        ("syncing", vec![Op::Obliterate(x, LOCAL)], &[0]),
        (
            "selected",
            vec![Op::Obliterate(x, LOCAL), Op::Compact],
            &[0, 1],
        ),
        (
            "stopped",
            vec![Op::Obliterate(x, LOCAL), Op::Compact],
            &[0, 1],
        ),
        ("serializing", vec![Op::Compact], &[1]),
        ("evicting", vec![Op::Compact], &[1]),
        ("serializing", vec![Op::Evict], &[1]),
        ("advanced", vec![Op::Store(2, 2)], &[6]),
        ("scanning", vec![Op::Store(2, 2)], &[6]),
        ("serializing", vec![Op::Store(2, 2)], &[6]),
        (
            "committing",
            vec![Op::Store(2, 2), Op::Store(4, 3)],
            &[6, 7],
        ),
        ("serializing", vec![Op::Swap(0, 1, 5)], &[5]),
    ];
    for (index, (point, call, changed)) in cases.into_iter().enumerate() {
        let case = format!("{point} in {}", call[0]);
        run(
            &clients.dir.join(format!("case{index}")),
            clients.config.clone(),
            |clients| {
                let Clients { a, b, c, .. } = clients;
                a.call(
                    "puts x, puts d durably and stores 1 for key 0",
                    &[put(x), Op::Put(d, DURABLE), Op::Store(0, 1)],
                    [1, 1],
                    &[Done, Done, Done],
                )?;
                let before = b.read("reads the baseline", &reads)?;
                a.pause(&case, point, &call)?;
                a.kill_and_restart()?;
                let after = b.read(&format!("reads after A was killed at {case}"), &reads)?;
                for (reader, step) in [(&mut *c, "reads"), (&mut *a, "reads, restarted")] {
                    let read = reader.read(step, &reads)?;
                    if read != after {
                        return Err(format!(
                            "{} {step} after A was killed at {case}: {read:?}, where B read {after:?}",
                            reader.name
                        ));
                    }
                }
                let damaged = after
                    .iter()
                    .any(|outcome| outcome.starts_with("wrong") || outcome.starts_with("failed"));
                let moved = (0..reads.len())
                    .any(|read| !changed.contains(&read) && after[read] != before[read]);
                match damaged || moved {
                    true => Err(format!(
                        "after A was killed at {case}, B read {after:?} where it read {before:?}"
                    )),
                    false => Ok(()),
                }
            },
        )
        .map_err(|err| format!("{case}: {err}"))?;
    }
    Ok(())
}

/// **A flush its caller dropped keeps the store claimed until the group flushes it left running
/// end**, so that another process neither writes the bucket under them nor has its write
/// replaced by theirs. C drops its flush once a group flush reaches its bucket write, which the
/// store runs on, held there until C's next request, and lets its claims and stores go.
fn a_dropped_flush_keeps_the_store_claimed_until_its_writes_end(
    clients: &mut Clients,
) -> Result<(), String> {
    let [x, y, z] = clients.keys(3, &mut HashSet::new())[..] else {
        return Err("three keys".to_string());
    };
    let Clients { a, b, c, .. } = clients;
    let reads = [get(x), get(y), get(z)];
    a.call("puts x", &[put(x)], [1, 1], &[Done])?;
    c.cancel(
        "puts y, its flush dropped at the bucket write",
        "serializing",
        &[put(y)],
        [1, 1],
        &[Done],
    )?;
    b.probed("starts putting z in the bucket", &[put(z)], [true, false])?;
    c.call(
        "lets the dropped flush go on, and reads x",
        &[get(x)],
        [1, 1],
        &[Found],
    )?;
    b.expect("puts z once C's flush ended", [1, 1], &[Done])?;
    a.call("reads x, y and z", &reads, [1, 0], &[Found, Found, Found])
}

/// **Kept stores whose directory was deleted, or deleted and created again, between calls read the
/// store found there now**, as a process opening the path afresh does, and every process's claims
/// exclude the others' on the files there now.
fn a_kept_store_follows_its_directory_deleted_and_created_again(
    clients: &mut Clients,
) -> Result<(), String> {
    let keys = clients.keys(4, &mut HashSet::new());
    let dir = clients.dir.clone();
    let config = clients.config.serialize();
    let Clients { a, b, c, .. } = clients;
    for (round, (x, y)) in [(keys[0], keys[1]), (keys[2], keys[3])]
        .into_iter()
        .enumerate()
    {
        let (first, second) = (2 * round as u64 + 1, 2 * round as u64 + 2);
        let reads = [get(x), get(y), Op::Load(0)];
        let opens = usize::from(round == 0);
        a.call(
            "puts x and stores a value for key 0",
            &[put(x), Op::Store(0, first)],
            [opens, opens],
            &[Done, Done],
        )?;
        b.call(
            "reads x and key 0",
            &reads,
            [1, 1],
            &[Found, Absent, Outcome::Value(first)],
        )?;
        let deleted = match round {
            0 => ["immutable", "mutable"]
                .iter()
                .try_for_each(|store| std::fs::remove_dir_all(dir.join(store))),
            _ => std::fs::remove_dir_all(&dir)
                .and_then(|()| std::fs::create_dir_all(&dir))
                .and_then(|()| std::fs::write(dir.join(CONFIG_FILE), &config)),
        };
        deleted.map_err(|err| format!("delete the store directories: {err}"))?;
        c.call(
            "reads where the stores were deleted",
            &reads,
            [1, 1],
            &[Absent, Absent, Absent],
        )?;
        c.call(
            "puts y and stores another value for key 0",
            &[put(y), Op::Store(0, second)],
            [1, 1],
            &[Done, Done],
        )?;
        let now = [Absent, Found, Outcome::Value(second)];
        a.call("reads the stores there now", &reads, [1, 1], &now)?;
        b.call("reads the stores there now", &reads, [1, 1], &now)?;
        b.hold("holds its claims", &[get(y)], [0, 0], &[Found])?;
        a.probed("starts reading", &[get(y)], [true, true])?;
        b.kill_and_restart()?;
        a.expect("reads once B was killed", [0, 0], &[Found])?;
    }
    Ok(())
}
