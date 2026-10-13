// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Counters, served remote payloads and hooks the read and write paths consult under
//! `test-util`, so a test can count the decodes, hashes and fetches a read costs, answer a remote
//! fetch without a remote, and order what concurrent reads and stores do.

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
