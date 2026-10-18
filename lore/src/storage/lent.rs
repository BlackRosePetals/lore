// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The buffers a call's items lend their reads through `data_out`.
//!
//! A read dropped part way may still write the buffer it was given until the I/O it started
//! completes, since no backend cancels an operation. So a call waits for every buffer it lent
//! before it completes, and a call that is itself dropped leaves its loan for [`await_abandoned`].

use std::future::Future;
use std::sync::Arc;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;

use lore_revision::event::LoreBytesMut;
use lore_storage::CallerBuffer;
use lore_storage::Lender;
use lore_storage::Loan;
use parking_lot::Mutex;

/// Loans on the buffers of calls dropped before every read let go of them.
static ABANDONED: Mutex<Vec<Loan>> = Mutex::new(Vec::new());

/// The buffers one call lends its reads, under one loan.
#[lore_macro::test_pub]
#[derive(Default)]
pub(crate) struct LentBuffers {
    lender: Option<Lender>,
    loan: Option<Loan>,
}

impl LentBuffers {
    /// The lender for one item's read into `out`, `None` when the item supplies no buffer. Every
    /// caller awaits [`Self::returned`] before its call completes, which keeps the buffer lent valid
    /// for as long as a read writes it.
    #[lore_macro::test_pub]
    pub(crate) fn lender(&mut self, out: &LoreBytesMut) -> Option<ItemLender> {
        if !out.is_supplied() {
            return None;
        }
        let lender = self.lender.get_or_insert_with(|| {
            let lender = Lender::new();
            self.loan = Some(lender.loan());
            lender
        });
        Some(ItemLender(lender.clone()))
    }

    /// Wait until no read writes any buffer lent.
    #[lore_macro::test_pub]
    pub(crate) async fn returned(&mut self) {
        self.lender = None;
        if let Some(loan) = &self.loan {
            loan.returned().await;
        }
        self.loan = None;
    }
}

/// Lends one item's `data_out` to its read, under the loan of the call's [`LentBuffers`].
#[lore_macro::test_pub]
pub(crate) struct ItemLender(Lender);

impl ItemLender {
    /// The buffer the item's read writes `out`, the `data_out` the lender was made for, through.
    #[lore_macro::test_pub]
    pub(crate) fn lend(self, out: &LoreBytesMut) -> CallerBuffer {
        // SAFETY: `out` names caller memory that stays valid, and reaches nobody else, until the
        // call completes, which awaits `LentBuffers::returned` and so the loan. `len` bounds the
        // read.
        unsafe { self.0.lend(out.ptr.cast::<u8>(), out.len) }
    }
}

impl Drop for LentBuffers {
    fn drop(&mut self) {
        self.lender = None;
        if let Some(loan) = self.loan.take()
            && !loan.is_returned()
        {
            let mut abandoned = ABANDONED.lock();
            abandoned.retain(|loan| !loan.is_returned());
            abandoned.push(loan);
        }
    }
}

/// Wakes the thread that made it, parked while it waits.
struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Wait, for at most `wait`, for every read still writing a buffer whose call was dropped.
/// Shutdown runs this once the runtime is gone, at best effort for a caller that shut down with
/// calls executing, which the C API does not allow: it covers the calls the runtime's shutdown
/// dropped, not one it left running past its timeout. With no runtime left to drive a timer, the
/// thread parks until a loan wakes it or the time runs out.
#[lore_macro::test_pub]
pub(crate) fn await_abandoned(wait: Duration) {
    let mut abandoned = std::mem::take(&mut *ABANDONED.lock());
    abandoned.retain(|loan| !loan.is_returned());
    if abandoned.is_empty() {
        return;
    }
    lore_base::lore_info!("Waiting for reads still writing the buffers of calls cut short");
    let deadline = Instant::now() + wait;
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    while let Some(loan) = abandoned.last() {
        if std::pin::pin!(loan.returned()).poll(&mut cx).is_ready() {
            abandoned.pop();
            continue;
        }
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            lore_base::lore_warn!(
                "Shutting down while reads of {} calls cut short still write their buffers",
                abandoned.len()
            );
            return;
        }
        std::thread::park_timeout(left);
    }
}
