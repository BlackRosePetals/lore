// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Counters, served remote payloads and hooks the read and write paths consult under
//! `test-util`, so a test can count the decodes, hashes and fetches a read costs, answer a remote
//! fetch without a remote, order what concurrent reads, stores and compactions do, fail packfile
//! writes and syncs or keep writes in flight, and interrupt a payload gate hold.

use std::cell::Cell;
use std::cell::RefCell;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Notify;

use crate::types::Address;
use crate::types::Fragment;
use crate::types::Hash;

thread_local! {
    static DECODES: Cell<usize> = const { Cell::new(0) };
    static HASHES: Cell<usize> = const { Cell::new(0) };
    static FETCHES: Cell<usize> = const { Cell::new(0) };
    static REMOTE: RefCell<Vec<(Address, Fragment, Bytes)>> = const { RefCell::new(Vec::new()) };
    static RESOLVED: RefCell<Vec<(Hash, Hash, Fragment, Bytes)>> =
        const { RefCell::new(Vec::new()) };
    static GATE: RefCell<Option<Arc<Notify>>> = const { RefCell::new(None) };
    static FOLLOWERS: Cell<usize> = const { Cell::new(0) };
    static MIDPOINT: RefCell<Option<Midpoint>> = const { RefCell::new(None) };
    static PAUSES: RefCell<Vec<(PausePoint, Arc<Pause>)>> = const { RefCell::new(Vec::new()) };
    static FAILED_WRITES: Cell<usize> = const { Cell::new(0) };
    static FAILED_SYNCS: Cell<usize> = const { Cell::new(0) };
    static IN_FLIGHT: RefCell<Option<Arc<InFlight>>> = const { RefCell::new(None) };
    static HOLD_INTERRUPT: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static PACKFILE_SIZE_LIMIT: Cell<u64> = const { Cell::new(0) };
}

/// A point where [`pause_at`] holds a store, a read or a compaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PausePoint {
    /// A packfile write has reserved its range and not yet written it.
    Reserved,
    /// A store has written its payload and not yet taken the bucket to insert its entry.
    Written,
    /// A read has its payload's location and has not yet loaded it.
    Located,
    /// Compaction has chosen the packfiles to rewrite and not yet stopped writes to them.
    Selected,
    /// Compaction has stopped writes to the packfiles it rewrites and not yet rewritten one.
    Stopped,
    /// A bucket write has copied the bucket and not yet written its file.
    Serializing,
    /// A sync of every packfile of a packstore has begun and synced none yet.
    Syncing,
}

/// A hold armed at a [`PausePoint`]: `reached` is notified when a task arrives there, which then
/// waits for `resume`.
#[derive(Default)]
pub struct Pause {
    pub reached: Notify,
    pub resume: Notify,
}

/// Holds the next task on this thread to reach `point` there until the returned pause is resumed.
/// Only that one task is held.
pub fn pause_at(point: PausePoint) -> Arc<Pause> {
    let pause = Arc::new(Pause::default());
    PAUSES.with(|pauses| pauses.borrow_mut().push((point, pause.clone())));
    pause
}

/// Holds this task at `point` if [`pause_at`] armed it on this thread.
pub(crate) async fn pause_point(point: PausePoint) {
    let pause = PAUSES.with(|pauses| {
        let mut pauses = pauses.borrow_mut();
        let position = pauses.iter().position(|(armed, _)| *armed == point)?;
        Some(pauses.swap_remove(position).1)
    });
    if let Some(pause) = pause {
        pause.reached.notify_one();
        pause.resume.notified().await;
    }
}

/// Makes the next `count` packfile writes on this thread fail.
pub fn fail_packfile_writes(count: usize) {
    FAILED_WRITES.with(|failed| failed.set(count));
}

/// Whether this packfile write fails, as [`fail_packfile_writes`] asked.
pub(crate) fn take_packfile_write_failure() -> bool {
    take_failure(&FAILED_WRITES)
}

/// Makes the next `count` packfile syncs on this thread fail, each once it has synced.
pub fn fail_packfile_syncs(count: usize) {
    FAILED_SYNCS.with(|failed| failed.set(count));
}

/// The result of this packfile sync, a failure if [`fail_packfile_syncs`] asked for one.
pub(crate) fn packfile_sync_result() -> std::io::Result<()> {
    if take_failure(&FAILED_SYNCS) {
        return Err(std::io::Error::other("packfile sync failed as asked"));
    }
    Ok(())
}

/// Whether this operation fails, taking one of the failures `failures` counts.
fn take_failure(failures: &'static std::thread::LocalKey<Cell<usize>>) -> bool {
    failures.with(|failed| {
        let count = failed.get();
        failed.set(count.saturating_sub(1));
        count > 0
    })
}

/// Makes packfile writes on this thread treat `limit` bytes as the size limit, so that a test
/// reaches the limit without writing gigabytes. 0 restores the real limit.
pub fn limit_packfile_size(limit: u64) {
    PACKFILE_SIZE_LIMIT.with(|size| size.set(limit));
}

/// The size limit [`limit_packfile_size`] set on this thread, if any.
pub(crate) fn packfile_size_limit() -> Option<u64> {
    PACKFILE_SIZE_LIMIT.with(|size| Some(size.get()).filter(|limit| *limit != 0))
}

/// A packfile write [`hold_packfile_write`] keeps in flight: it never completes, and the buffer it
/// was issued with is kept until [`InFlight::complete`], as a driver keeps it until the kernel is
/// done with the write. `issued` is notified once the write is issued.
#[derive(Default)]
pub struct InFlight {
    pub issued: Notify,
    buffer: parking_lot::Mutex<Option<Box<dyn std::any::Any + Send>>>,
}

impl InFlight {
    /// Drops the buffer the write was issued with, as the driver does once the write completes.
    pub fn complete(&self) {
        drop(self.buffer.lock().take());
    }
}

/// Keeps the next packfile write on this thread in flight, see [`InFlight`].
pub fn hold_packfile_write() -> Arc<InFlight> {
    let in_flight = Arc::new(InFlight::default());
    IN_FLIGHT.with(|held| *held.borrow_mut() = Some(in_flight.clone()));
    in_flight
}

/// Parks this packfile write for good, keeping a copy of `buffer` in the [`InFlight`] that
/// [`hold_packfile_write`] armed on this thread, if any.
pub(crate) async fn park_packfile_write<B: Clone + Send + 'static>(buffer: &B) {
    let Some(in_flight) = IN_FLIGHT.with(|held| held.borrow_mut().take()) else {
        return;
    };
    *in_flight.buffer.lock() = Some(Box::new(buffer.clone()));
    in_flight.issued.notify_one();
    std::future::pending::<()>().await;
}

/// Runs `hook` once, in the next payload gate hold taken on this thread, after the hold reads the
/// epoch and before it counts itself.
pub fn interrupt_next_hold(hook: impl FnOnce() + 'static) {
    HOLD_INTERRUPT.with(|interrupt| *interrupt.borrow_mut() = Some(Box::new(hook)));
}

/// Runs the hook [`interrupt_next_hold`] set on this thread, if any.
pub(crate) fn hold_interrupted() {
    let hook = HOLD_INTERRUPT.with(|interrupt| interrupt.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

type Midpoint = Box<dyn Fn(Address, bool)>;

/// Records one payload decode on this thread.
pub fn count_decode() {
    DECODES.with(|count| count.set(count.get() + 1));
}

/// Records one content hash on this thread.
pub fn count_hash() {
    HASHES.with(|count| count.set(count.get() + 1));
}

/// Remote fetches, decodes, and hashes recorded on this thread since the last [`reset_counts`].
pub fn counts() -> (usize, usize, usize) {
    (
        FETCHES.with(Cell::get),
        DECODES.with(Cell::get),
        HASHES.with(Cell::get),
    )
}

/// Zeroes the counts [`counts`] reports on this thread.
pub fn reset_counts() {
    FETCHES.with(|count| count.set(0));
    DECODES.with(|count| count.set(0));
    HASHES.with(|count| count.set(0));
}

/// Makes a remote fetch of `address` on this thread answer with `fragment` and `payload`.
pub fn serve_remote(address: Address, fragment: Fragment, payload: Bytes) {
    REMOTE.with(|remote| remote.borrow_mut().push((address, fragment, payload)));
}

/// What [`serve_remote`] last set up for `address` on this thread, counted as one remote fetch.
pub fn remote_payload(address: Address) -> Option<(Fragment, Bytes)> {
    FETCHES.with(|count| count.set(count.get() + 1));
    REMOTE.with(|remote| {
        remote
            .borrow()
            .iter()
            .rfind(|(served, _, _)| *served == address)
            .map(|(_, fragment, payload)| (*fragment, payload.clone()))
    })
}

/// Makes every served remote fetch on this thread wait for one notification of `gate` before it
/// answers.
pub fn hold_remote_fetches(gate: Arc<Notify>) {
    GATE.with(|held| *held.borrow_mut() = Some(gate));
}

/// Waits for the gate [`hold_remote_fetches`] set on this thread, if any.
pub(crate) async fn remote_gate() {
    let gate = GATE.with(|held| held.borrow().clone());
    if let Some(gate) = gate {
        gate.notified().await;
    }
}

/// Records one caller joining a read flight as a follower on this thread.
pub(crate) fn count_follower() {
    FOLLOWERS.with(|count| count.set(count.get() + 1));
}

/// Callers that joined a read flight as followers on this thread.
pub fn followers() -> usize {
    FOLLOWERS.with(Cell::get)
}

/// Runs `hook` inside every store in-flight guard this thread drops, between its landed count and
/// the removal of its entry, with the guard's address and whether its entry is still in flight,
/// until [`clear_guard_midpoint`].
pub fn set_guard_midpoint(hook: impl Fn(Address, bool) + 'static) {
    MIDPOINT.with(|midpoint| *midpoint.borrow_mut() = Some(Box::new(hook)));
}

/// Stops running the hook [`set_guard_midpoint`] set on this thread.
pub fn clear_guard_midpoint() {
    MIDPOINT.with(|midpoint| *midpoint.borrow_mut() = None);
}

/// Runs the hook [`set_guard_midpoint`] set on this thread for a guard of `address`, telling it
/// whether the entry is still in flight as `in_flight` answers.
pub(crate) fn guard_midpoint(address: Address, in_flight: impl FnOnce() -> bool) {
    MIDPOINT.with(|midpoint| {
        if let Some(hook) = midpoint.borrow().as_ref() {
            hook(address, in_flight());
        }
    });
}

/// Makes a remote resolve of `key` on this thread answer with `resolved` and its root.
pub fn serve_remote_resolved(key: Hash, resolved: Hash, fragment: Fragment, payload: Bytes) {
    RESOLVED.with(|remote| {
        remote.borrow_mut().push((key, resolved, fragment, payload));
    });
}

/// What [`serve_remote_resolved`] last set up for `key` on this thread, counted as one remote
/// fetch.
pub fn remote_resolved(key: Hash) -> Option<(Hash, Fragment, Bytes)> {
    FETCHES.with(|count| count.set(count.get() + 1));
    RESOLVED.with(|remote| {
        remote
            .borrow()
            .iter()
            .rfind(|(served, ..)| *served == key)
            .map(|(_, resolved, fragment, payload)| (*resolved, *fragment, payload.clone()))
    })
}
