// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::task::Context;
use std::task::Waker;
use std::time::Duration;

use lore_storage::local::immutable_store::payload_gate::PayloadGate;
use lore_storage::test_util::interrupt_next_hold;

use crate::store_fixture::wait_until;

/// A drain of `gate` in a task of its own.
fn spawn_drain(gate: &Arc<PayloadGate>) -> tokio::task::JoinHandle<()> {
    let gate = gate.clone();
    lore_base::lore_spawn!(async move { gate.drain().await })
}

/// A drain waits for a hold taken before it, and returns once the hold is released.
#[tokio::test]
async fn a_drain_waits_for_a_hold_taken_before_it() {
    let gate = Arc::new(PayloadGate::default());
    let hold = gate.hold();

    let drain = spawn_drain(&gate);
    wait_until(|| gate.is_draining(), &drain).await;
    assert!(!drain.is_finished(), "the drain waits for the hold");

    drop(hold);
    drain.await.unwrap();
}

/// A drain after a drain cancelled while it waited still waits for the holds taken before the
/// cancelled one.
#[tokio::test]
async fn a_drain_after_a_cancelled_drain_waits_for_the_holds_before_it() {
    let gate = Arc::new(PayloadGate::default());
    let hold = gate.hold();
    let cancelled = spawn_drain(&gate);
    wait_until(|| gate.is_draining(), &cancelled).await;
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());

    let drain = spawn_drain(&gate);
    wait_until(|| gate.is_draining(), &drain).await;
    assert!(!drain.is_finished(), "the drain waits for the hold");

    drop(hold);
    drain.await.unwrap();
}

/// A hold taken during a drain neither waits for it nor holds it up: the drain returns once the
/// holds taken before it are released, with the later one still taken.
#[tokio::test]
async fn a_hold_taken_during_a_drain_does_not_hold_it_up() {
    let gate = Arc::new(PayloadGate::default());
    let before = gate.hold();
    let drain = spawn_drain(&gate);
    wait_until(|| gate.is_draining(), &drain).await;

    let during = gate.hold();
    drop(before);

    tokio::time::timeout(Duration::from_secs(10), drain)
        .await
        .expect("the drain does not wait for the later hold")
        .unwrap();
    drop(during);
    tokio::time::timeout(Duration::from_secs(10), gate.drain())
        .await
        .expect("a drain with every hold released returns");
}

/// A hold that reads the epoch before a drain begins, and counts itself while the drain waits,
/// counts itself again under the drain's epoch, so that the drain does not wait for it.
#[tokio::test]
async fn a_hold_that_read_the_epoch_before_a_drain_does_not_hold_it_up() {
    let gate = Arc::new(PayloadGate::default());
    let before = gate.hold();
    let (begun, drain) = std::sync::mpsc::channel();
    {
        let gate = gate.clone();
        interrupt_next_hold(move || {
            let mut drain = Box::pin(async move { gate.drain().await });
            let waiting = drain
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending();
            assert!(waiting, "the drain waits for the hold taken before it");
            begun.send(drain).unwrap();
        });
    }
    let during = gate.hold();
    drop(before);

    tokio::time::timeout(Duration::from_secs(10), drain.try_recv().unwrap())
        .await
        .expect("the drain does not wait for the later hold");
    drop(during);
}

/// A hold that reads the epoch before a drain begins, and counts itself once the drain has
/// returned, is waited for by the next drain.
#[tokio::test]
async fn a_hold_counted_after_a_drain_is_waited_for_by_the_next() {
    let gate = Arc::new(PayloadGate::default());
    {
        let gate = gate.clone();
        interrupt_next_hold(move || futures::executor::block_on(gate.drain()));
    }
    let hold = gate.hold();

    let drain = spawn_drain(&gate);
    wait_until(|| gate.is_draining(), &drain).await;
    assert!(!drain.is_finished(), "the next drain waits for the hold");

    drop(hold);
    drain.await.unwrap();
}
