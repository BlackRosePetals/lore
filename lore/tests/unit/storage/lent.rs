// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::time::Duration;

use lore::storage::lent::LentBuffers;
use lore_revision::event::LoreBytesMut;
use lore_storage::CallerBuffer;

/// Lend `buffer` to an item's read through `lent`.
fn lend(lent: &mut LentBuffers, buffer: &mut [u8]) -> CallerBuffer {
    let out = LoreBytesMut {
        ptr: buffer.as_mut_ptr().cast(),
        len: buffer.len(),
    };
    lent.lender(&out)
        .expect("an item supplying a buffer gets a lender")
        .lend(&out)
}

/// A call waits for every buffer it lent, so it completes only once the last read holding one,
/// as an I/O operation does past a dropped read, has let go of it.
#[tokio::test]
async fn a_call_waits_for_every_buffer_it_lent() {
    let mut first = vec![0u8; 16];
    let mut second = vec![0u8; 16];
    let mut lent = LentBuffers::default();
    let mut held_first = lend(&mut lent, &mut first);
    let mut held_second = lend(&mut lent, &mut second);

    held_first.as_mut_slice().fill(1);
    drop(held_first);
    let (release, released) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        released.recv().expect("the holder is released");
        held_second.as_mut_slice().fill(2);
    });
    let mut returned = std::pin::pin!(lent.returned());
    assert!(
        futures::poll!(returned.as_mut()).is_pending(),
        "the buffers came back while a read held one"
    );
    release.send(()).expect("release the holder");
    tokio::time::timeout(Duration::from_secs(5), returned)
        .await
        .expect("the buffers come back once the holder lets go");

    assert!(first.iter().all(|byte| *byte == 1));
    assert!(
        second.iter().all(|byte| *byte == 2),
        "the call completed before the holder's write"
    );
    holder.join().expect("the holder finishes");
}
