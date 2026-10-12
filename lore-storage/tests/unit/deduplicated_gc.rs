// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Eviction and compaction of a client or server store over every combination of up to three
//! entries of one content, across two partitions and two contexts, each held locally or durably
//! at one of two copies of the payload, evicted, never stored, or obliterated.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic;

use bytes::Bytes;
use lore_storage::Address;
use lore_storage::Context;
use lore_storage::FragmentFlags;
use lore_storage::Hash;
use lore_storage::Partition;
use lore_storage::hash;
use lore_storage::immutable_store::ImmutableStore;
use lore_storage::local::immutable_store::ImmutableData;
use lore_storage::local::immutable_store::ImmutableStoreEntry;
use lore_storage::local::immutable_store::ImmutableStoreSettings;
use lore_storage::local::immutable_store::LocalImmutableStore;

use crate::store_fixture::assert_payloads_read_back;
use crate::store_fixture::compact_group_toward;
use crate::store_fixture::group_entries;
use crate::store_fixture::payloads_in_one_group;

/// How an entry holds its content.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Held {
    /// Locally only, at the first or the second copy of the payload.
    Local(usize),
    /// Durably, and locally at the first or the second copy of the payload.
    Durable(usize),
    /// Durably, with the local payload evicted.
    Evicted,
    /// Neither durably nor locally.
    Unstored,
    /// As a tombstone of obliterated content.
    Obliterated,
}

const HELD: [Held; 7] = [
    Held::Local(0),
    Held::Local(1),
    Held::Durable(0),
    Held::Durable(1),
    Held::Evicted,
    Held::Unstored,
    Held::Obliterated,
];

/// Every sequence of one to three ways of holding a content.
fn combinations() -> Vec<Vec<Held>> {
    let mut combinations: Vec<Vec<Held>> = HELD.iter().map(|held| vec![*held]).collect();
    let mut shorter = 0..combinations.len();
    for _ in 1..3 {
        let end = combinations.len();
        for index in shorter {
            for held in HELD {
                let mut combination = combinations[index].clone();
                combination.push(held);
                combinations.push(combination);
            }
        }
        shorter = end..combinations.len();
    }
    combinations
}

/// The partition and context of the entry at `index` of a combination. The second entry shares
/// the first one's context in another partition, and the third shares the first one's partition
/// under another context.
fn place(index: usize) -> (Partition, Context) {
    match index {
        0 => (Partition::from([0xa1; 16]), Context::from([0x01; 16])),
        1 => (Partition::from([0xb2; 16]), Context::from([0x01; 16])),
        _ => (Partition::from([0xa1; 16]), Context::from([0x02; 16])),
    }
}

/// The data of an entry holding a content of `size` bytes as `held`, with the copies of its
/// payload at `copies`.
fn held_data(held: Held, size: u32, copies: [(u32, u32); 2]) -> ImmutableData {
    let local = FragmentFlags::PayloadStoredLocal.bits();
    let durable = FragmentFlags::PayloadStoredDurable.bits();
    let (flags, (pack_file, pack_offset)) = match held {
        Held::Local(copy) => (local, copies[copy]),
        Held::Durable(copy) => (local | durable, copies[copy]),
        Held::Evicted => (durable, (0, 0)),
        Held::Unstored => (0, (0, 0)),
        Held::Obliterated => {
            return ImmutableData {
                flags: FragmentFlags::PayloadObliterated.bits(),
                ..Default::default()
            };
        }
    };
    ImmutableData {
        flags,
        size_payload: size,
        size_content: size as u64,
        pack_offset,
        pack_file,
        last_access: 0,
    }
}

/// How a pass collects garbage: with capacity eviction before compaction or not, and in a store
/// that protects local payloads, as a client store does, or not, as a server store does.
#[derive(Clone, Copy, Debug)]
struct Pass {
    capacity: bool,
    protect: bool,
}

/// A store in `dir` protecting local payloads as `pass` says, with its whole index loaded.
async fn open_store(dir: &Path, pass: Pass) -> Arc<LocalImmutableStore> {
    let store = LocalImmutableStore::new(
        Some(dir.to_path_buf()),
        ImmutableStoreSettings {
            protect_local_fragment: pass.protect,
            implicit_durable_stored: false,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    store.clone().load_index().await.unwrap();
    store
}

/// The data of every entry of the group, by hash, partition and context.
async fn entries_by_address(
    store: &LocalImmutableStore,
    group_index: usize,
) -> BTreeMap<(Hash, Partition, Context), ImmutableData> {
    group_entries(store, group_index)
        .await
        .into_iter()
        .map(|entry| {
            (
                (entry.address.hash, entry.partition, entry.address.context),
                entry.data,
            )
        })
        .collect()
}

/// A store in `dir`, as `pass` says, with each of `combinations` as the entries of a content of
/// its own, in one group, and the copies of each payload its entries hold. Returns the store,
/// the group and the payload of each combination.
async fn store_holding(
    dir: &Path,
    combinations: &[Vec<Held>],
    pass: Pass,
) -> (Arc<LocalImmutableStore>, usize, Vec<Vec<u8>>) {
    let store = open_store(dir, pass).await;
    let (group_index, payloads) = payloads_in_one_group(combinations.len());
    let group = &store.group[group_index];
    assert_eq!(group.bucket_count.load(atomic::Ordering::Relaxed), 1);

    let mut entries = vec![];
    for (combination, payload) in combinations.iter().zip(&payloads) {
        let mut copies = [(0, 0); 2];
        for (copy, location) in copies.iter_mut().enumerate() {
            if combination.contains(&Held::Local(copy))
                || combination.contains(&Held::Durable(copy))
            {
                let stored = group
                    .packstore
                    .store(Bytes::copy_from_slice(payload))
                    .await
                    .unwrap();
                *location = (stored.id, stored.offset);
            }
        }
        let hash = hash::hash_slice(payload);
        for (index, held) in combination.iter().enumerate() {
            let (partition, context) = place(index);
            entries.push(ImmutableStoreEntry {
                address: Address { hash, context },
                partition,
                data: held_data(*held, payload.len() as u32, copies),
            });
        }
    }
    entries
        .sort_unstable_by_key(|entry| (entry.address.hash, entry.partition, entry.address.context));

    let mut bucket = group.bucket(0).write().await;
    assert!(bucket.entry.is_empty());
    for (index, entry) in entries.into_iter().enumerate() {
        bucket.entry.push(entry);
        bucket.sorted_index.push(index as u32);
    }
    group.dirty[0].store(true, atomic::Ordering::Relaxed);
    drop(bucket);
    (store, group_index, payloads)
}

/// What `pass` leaves of an entry holding its content as `held`, in a combination holding it as
/// `combination`, with `before` its data: `None` when capacity eviction removes it. Compaction
/// may move a payload it leaves in place.
fn expected(
    combination: &[Held],
    held: Held,
    before: ImmutableData,
    pass: Pass,
) -> Option<ImmutableData> {
    let evicted = ImmutableData {
        flags: before.flags & !FragmentFlags::PayloadStoredLocal.bits(),
        pack_file: 0,
        pack_offset: 0,
        ..before
    };
    match held {
        _ if pass.capacity && !pass.protect => None,
        Held::Durable(_) | Held::Evicted if pass.capacity => None,
        Held::Local(_) | Held::Durable(_) if !pass.protect => Some(evicted),
        Held::Durable(copy) if !combination.contains(&Held::Local(copy)) => Some(evicted),
        _ => Some(before),
    }
}

/// Require each entry of `combinations` in `store` to be what [`expected`] leaves of it, every
/// payload an entry holds to read back from where it points, and the entries that shared a
/// payload to still share one. `before` is each entry's data before eviction, by address. Returns
/// how many entries compaction moved.
async fn assert_held(
    store: &Arc<LocalImmutableStore>,
    group_index: usize,
    combinations: &[Vec<Held>],
    payloads: &[Vec<u8>],
    before: &BTreeMap<(Hash, Partition, Context), ImmutableData>,
    pass: Pass,
) -> usize {
    let mut after = entries_by_address(store, group_index).await;
    let packstore = &store.group[group_index].packstore;
    let mut moved = 0;
    for (combination, payload) in combinations.iter().zip(payloads) {
        let hash = hash::hash_slice(payload);
        let mut locations = vec![];
        for (index, held) in combination.iter().enumerate() {
            let (partition, context) = place(index);
            let key = (hash, partition, context);
            let left = after.remove(&key);
            let Some(expected) = expected(combination, *held, before[&key], pass) else {
                assert_eq!(left, None, "{combination:?} entry {index} is evicted");
                continue;
            };
            let left = left.unwrap_or_else(|| panic!("{combination:?} entry {index} is kept"));
            if expected.pack_file == 0 {
                assert_eq!(left, expected, "{combination:?} entry {index}");
                continue;
            }
            assert_eq!(
                ImmutableData {
                    pack_file: expected.pack_file,
                    pack_offset: expected.pack_offset,
                    ..left
                },
                expected,
                "{combination:?} entry {index} changes only where its payload is"
            );
            assert_eq!(
                packstore
                    .load(left.pack_file, left.pack_offset, left.size_payload)
                    .await
                    .unwrap()
                    .as_ref(),
                payload.as_slice(),
                "{combination:?} entry {index} points at its content"
            );
            if *held == Held::Local(0) || *held == Held::Local(1) {
                assert_payloads_read_back(
                    store,
                    partition,
                    [(Address { hash, context }, payload.clone())],
                )
                .await;
            }
            let location = (left.pack_file, left.pack_offset);
            moved += usize::from(location != (expected.pack_file, expected.pack_offset));
            locations.push(((expected.pack_file, expected.pack_offset), location));
        }
        for (before, after) in &locations {
            for (other_before, other_after) in &locations {
                assert!(
                    before != other_before || after == other_after,
                    "{combination:?} entries sharing a payload still share one"
                );
            }
        }
    }
    assert!(after.is_empty(), "no other entry is left: {after:?}");
    moved
}

/// Run `pass` over a store holding every combination, then reopen the store and check it again.
async fn collect_every_combination(dir_prefix: &str, pass: Pass) {
    let combinations = combinations();
    assert_eq!(combinations.len(), 7 + 7 * 7 + 7 * 7 * 7);
    for (chunk_index, chunk) in combinations.chunks(100).enumerate() {
        let dir = lore_base::test_util::TempDir::new(&format!("{dir_prefix}{chunk_index}_"));
        let (store, group_index, payloads) = store_holding(dir.as_ref(), chunk, pass).await;
        let before = entries_by_address(&store, group_index).await;

        if pass.capacity {
            store.clone().evict(1, false, None).await.unwrap();
        }
        assert!(compact_group_toward(&store, group_index, 1, pass.protect).await);
        let moved = assert_held(&store, group_index, chunk, &payloads, &before, pass).await;
        if pass.protect {
            assert!(moved > 0, "compaction rewrites the packfile");
        } else {
            assert_eq!(
                store.group[group_index]
                    .packstore
                    .total_size()
                    .await
                    .unwrap(),
                0,
                "a server store reclaims every payload"
            );
        }

        drop(store);
        let store = open_store(dir.as_ref(), pass).await;
        assert_held(&store, group_index, chunk, &payloads, &before, pass).await;
    }
}

/// Size eviction evicts a payload only when every entry at its location is durable, compaction
/// moves every entry at a location it keeps together and changes nothing else, and an entry
/// that holds no payload is left as it is, in every combination.
#[tokio::test]
async fn size_eviction_and_compaction_keep_every_combination_of_entries_consistent() {
    let pass = Pass {
        capacity: false,
        protect: true,
    };
    collect_every_combination("dedup_gc_size_", pass).await;
}

/// Capacity eviction removes every durable entry, whatever else holds its payload, and leaves
/// every other entry, which compaction then moves together, in every combination.
#[tokio::test]
async fn capacity_eviction_and_compaction_keep_every_combination_of_entries_consistent() {
    let pass = Pass {
        capacity: true,
        protect: true,
    };
    collect_every_combination("dedup_gc_capacity_", pass).await;
}

/// A server store, which protects nothing, evicts every payload by size and leaves every entry
/// without one, and compaction reclaims every byte, in every combination.
#[tokio::test]
async fn server_size_eviction_reclaims_every_combination() {
    let pass = Pass {
        capacity: false,
        protect: false,
    };
    collect_every_combination("dedup_gc_server_size_", pass).await;
}

/// A server store's capacity eviction removes every entry, tombstones included, and compaction
/// reclaims every byte, in every combination.
#[tokio::test]
async fn server_capacity_eviction_removes_every_combination() {
    let pass = Pass {
        capacity: true,
        protect: false,
    };
    collect_every_combination("dedup_gc_server_capacity_", pass).await;
}
