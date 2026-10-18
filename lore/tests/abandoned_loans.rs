// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Shutdown waits for the buffers of a call dropped while a read still held one.
//!
//! In its own binary: it empties the process-wide list of abandoned loans.

use std::sync::mpsc::Receiver;
use std::time::Duration;

use lore::storage::lent::LentBuffers;
use lore::storage::lent::await_abandoned;
use lore_revision::event::LoreBytesMut;
use lore_storage::CallerBuffer;

/// Lend `buffer` to a read of a call that is then dropped, returning the read's handle.
fn lend_to_dropped_call(buffer: &mut [u8]) -> CallerBuffer {
    let out = LoreBytesMut {
        ptr: buffer.as_mut_ptr().cast(),
        len: buffer.len(),
    };
    LentBuffers::default()
        .lender(&out)
        .expect("an item supplying a buffer gets a lender")
        .lend(&out)
}

/// Run `await_abandoned(wait)` on a thread of its own, reporting on the receiver when it returns.
fn await_abandoned_on_a_thread(wait: Duration) -> (Receiver<()>, std::thread::JoinHandle<()>) {
    let (done, waited) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        await_abandoned(wait);
        done.send(()).expect("report the wait");
    });
    (waited, waiter)
}

/// Shutdown waits for the buffer of a call dropped while a read still holds it, and for no longer
/// than it is given. One test, as both parts take the process-wide list.
#[test]
fn shutdown_waits_for_the_buffers_of_a_dropped_call_for_at_most_its_bound() {
    let mut kept = vec![0u8; 16];
    let held_past_the_bound = lend_to_dropped_call(&mut kept);
    let (waited, waiter) = await_abandoned_on_a_thread(Duration::from_millis(10));
    waited
        .recv_timeout(Duration::from_secs(5))
        .expect("shutdown returns once its bound runs out");
    waiter.join().expect("the waiter finishes");
    drop(held_past_the_bound);

    let mut buffer = vec![0u8; 16];
    let mut held = lend_to_dropped_call(&mut buffer);
    let (release, released) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        released.recv().expect("the holder is released");
        held.as_mut_slice().fill(3);
    });
    let (waited, waiter) = await_abandoned_on_a_thread(Duration::from_secs(5));
    assert!(
        waited.recv_timeout(Duration::from_millis(10)).is_err(),
        "shutdown returned while a read held the buffer"
    );
    release.send(()).expect("release the holder");
    waited
        .recv_timeout(Duration::from_secs(5))
        .expect("shutdown returns once the holder lets go");

    assert!(buffer.iter().all(|byte| *byte == 3));
    holder.join().expect("the holder finishes");
    waiter.join().expect("the waiter finishes");
}
