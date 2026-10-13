// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_storage::Address;
use lore_storage::Context;
use lore_storage::Hash;
use lore_storage::Partition;
use lore_storage::error::StorageError;
use lore_storage::errors::AddressNotFound;
use lore_storage::read_flight::*;

fn test_key(seed: u8) -> (Partition, Address) {
    let partition = Partition::from([seed; 16]);
    let address = Address {
        hash: Hash::from([seed; 32]),
        context: Context::from([seed; 16]),
    };
    (partition, address)
}

#[tokio::test]
async fn first_caller_is_leader_rest_are_followers() {
    let (partition, address) = test_key(1);
    let leader = match join_read_flight(partition, address, false) {
        ReadFlight::Leader(guard) => guard,
        ReadFlight::Follower(_) => panic!("first caller must be the leader"),
    };
    for _ in 0..3 {
        match join_read_flight(partition, address, false) {
            ReadFlight::Follower(_) => {}
            ReadFlight::Leader(_) => {
                panic!("caller joining an active flight must be a follower")
            }
        }
    }
    drop(leader);
}

#[tokio::test]
async fn followers_see_leader_error() {
    let (partition, address) = test_key(2);
    let leader = match join_read_flight(partition, address, false) {
        ReadFlight::Leader(guard) => guard,
        ReadFlight::Follower(_) => panic!("first caller must be the leader"),
    };
    let follower_rx = match join_read_flight(partition, address, false) {
        ReadFlight::Follower(rx) => rx,
        ReadFlight::Leader(_) => panic!("second caller must be a follower"),
    };
    let waiter = lore_base::lore_spawn!(await_flight(follower_rx));
    let expected = StorageError::from(AddressNotFound::from(address));
    leader.complete(&Err(expected.clone()));

    let observed = waiter.await.expect("await_flight task panicked");
    assert!(
        matches!(observed, Some(Err(StorageError::AddressNotFound(_)))),
        "expected follower to observe the leader's error, got {observed:?}"
    );
}

#[tokio::test]
async fn dropped_leader_without_result_promotes_a_follower() {
    let (partition, address) = test_key(3);
    let leader = match join_read_flight(partition, address, false) {
        ReadFlight::Leader(guard) => guard,
        ReadFlight::Follower(_) => panic!("first caller must be the leader"),
    };
    let follower_rx = match join_read_flight(partition, address, false) {
        ReadFlight::Follower(rx) => rx,
        ReadFlight::Leader(_) => panic!("second caller must be a follower"),
    };

    drop(leader);

    assert!(
        await_flight(follower_rx).await.is_none(),
        "a follower must see `None` when the leader vanishes without a result"
    );

    match join_read_flight(partition, address, false) {
        ReadFlight::Leader(guard) => drop(guard),
        ReadFlight::Follower(_) => panic!("promoted caller must become the new leader"),
    }
}

#[tokio::test]
async fn a_cancelled_follower_releases_its_receiver() {
    let (partition, address) = test_key(4);
    let leader = match join_read_flight(partition, address, false) {
        ReadFlight::Leader(guard) => guard,
        ReadFlight::Follower(_) => panic!("first caller must be the leader"),
    };
    let follower_rx = match join_read_flight(partition, address, false) {
        ReadFlight::Follower(rx) => rx,
        ReadFlight::Leader(_) => panic!("second caller must be a follower"),
    };

    let follower_task = lore_base::lore_spawn!(await_flight(follower_rx));
    assert!(leader.has_followers());
    follower_task.abort();
    let _ = follower_task.await;

    assert!(
        !leader.has_followers(),
        "the cancelled follower released its receiver"
    );
    drop(leader);
}

/// A read of the same content under another context or partition follows the flight where the
/// store does not isolate partitions, and is told the leader's address is not its own.
#[tokio::test]
async fn a_read_of_the_same_content_under_another_address_follows() {
    let (partition, address) = test_key(5);
    let leader = match join_read_flight(partition, address, false) {
        ReadFlight::Leader(guard) => guard,
        ReadFlight::Follower(_) => panic!("first caller must be the leader"),
    };
    let other_partition = Partition::from([6; 16]);
    let other_address = Address {
        context: Context::from([6; 16]),
        ..address
    };
    match join_read_flight(other_partition, other_address, false) {
        ReadFlight::Follower(follower) => {
            assert!(!follower.led_by(other_partition, other_address));
            assert!(follower.led_by(partition, address));
        }
        ReadFlight::Leader(_) => panic!("a read of the same content must follow"),
    }
    drop(leader);
}

/// Where the store isolates partitions, a read of the same content in another partition leads a
/// flight of its own, and one under another context in the same partition follows.
#[tokio::test]
async fn an_isolating_store_keys_reads_by_partition() {
    let (partition, address) = test_key(7);
    let leader = match join_read_flight(partition, address, true) {
        ReadFlight::Leader(guard) => guard,
        ReadFlight::Follower(_) => panic!("first caller must be the leader"),
    };
    let other_partition = Partition::from([8; 16]);
    let other_leader = match join_read_flight(other_partition, address, true) {
        ReadFlight::Leader(guard) => guard,
        ReadFlight::Follower(_) => panic!("another partition must lead its own flight"),
    };
    let other_context = Address {
        context: Context::from([8; 16]),
        ..address
    };
    assert!(matches!(
        join_read_flight(partition, other_context, true),
        ReadFlight::Follower(_)
    ));
    drop(other_leader);
    drop(leader);
}

/// A leader no follower joins allocates nothing: the channel its outcome would go out on is the
/// first follower's to create.
#[test]
fn a_leader_without_followers_allocates_nothing() {
    let partition = Partition::from([9; 16]);
    let addresses: Vec<Address> = (0..64u8)
        .map(|i| Address {
            hash: Hash::from([0x90 ^ i; 32]),
            context: Context::from([9; 16]),
        })
        .collect();
    let failed = Err(StorageError::from(AddressNotFound::from(addresses[0])));
    let lead = |address: Address| match join_read_flight(partition, address, false) {
        ReadFlight::Leader(guard) => guard.complete(&failed),
        ReadFlight::Follower(_) => panic!("no flight for this content is in flight"),
    };
    addresses.iter().copied().for_each(lead);

    lore_base::allocator::set_allocation_counting(true);
    let before = lore_base::allocator::thread_allocation_count();
    addresses.iter().copied().for_each(lead);
    let allocations = lore_base::allocator::thread_allocation_count() - before;
    lore_base::allocator::set_allocation_counting(false);
    assert_eq!(allocations, 0, "leaders allocated");
}

/// The first follower of a flight creates its channel, and a later follower subscribes to it
/// without allocating.
#[test]
fn a_second_follower_subscribes_without_allocating() {
    let (partition, address) = test_key(10);
    let leader = match join_read_flight(partition, address, false) {
        ReadFlight::Leader(guard) => guard,
        ReadFlight::Follower(_) => panic!("first caller must be the leader"),
    };
    let first = match join_read_flight(partition, address, false) {
        ReadFlight::Follower(follower) => follower,
        ReadFlight::Leader(_) => panic!("second caller must be a follower"),
    };
    assert!(leader.has_followers());

    lore_base::allocator::set_allocation_counting(true);
    let before = lore_base::allocator::thread_allocation_count();
    let second = join_read_flight(partition, address, false);
    let allocations = lore_base::allocator::thread_allocation_count() - before;
    lore_base::allocator::set_allocation_counting(false);
    assert!(matches!(second, ReadFlight::Follower(_)));
    assert_eq!(allocations, 0, "the second follower allocated");
    drop((first, second, leader));
}
