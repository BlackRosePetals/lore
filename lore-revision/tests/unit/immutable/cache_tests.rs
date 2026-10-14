// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Fragment;
use lore_base::types::FragmentFlags;
use lore_base::types::FragmentReference;
use lore_base::types::Hash;
use lore_base::types::HealResult;
use lore_base::types::Partition;
use lore_base::types::TypedBytes;
use lore_base::types::VerifyResult;
use lore_revision::immutable::*;
use lore_revision::repository::RemoteState;
use lore_revision::repository::RepositoryContext;
use lore_storage::options::ReadOptions;
use lore_storage::options::WriteOptions;
use lore_transport::Connection;
use lore_transport::ProtocolError;
use lore_transport::StorageSession;

use super::session_tests::context_with_state;
use super::session_tests::under_execution_context;

/// How a [`ServingStorage`] departs from serving what it holds: the address it answers with its
/// payload altered, until a heal where it `heals`; the address whose first get it answers
/// `SlowDown`; the gate it holds each get at until as many as the gate takes are in; and the
/// content whose first get it holds for a notification.
#[derive(Default)]
struct Serving {
    tampered: Option<Address>,
    heals: bool,
    slow_down: Option<Address>,
    gate: Option<Arc<tokio::sync::Barrier>>,
    hold: Option<(Hash, Arc<tokio::sync::Notify>)>,
}

/// A remote serving what `source` stores as `serving` says, recording each address asked of it and
/// the most gets it held at once.
struct ServingStorage {
    source: Arc<RepositoryContext>,
    serving: Serving,
    asked: std::sync::Mutex<Vec<Address>>,
    healed: AtomicBool,
    slowed: AtomicBool,
    in_flight: AtomicUsize,
    peak: AtomicUsize,
}

#[async_trait::async_trait]
impl lore_transport::Storage for ServingStorage {
    async fn session_start(
        &self,
        _partition: Partition,
        _correlation_id: &str,
    ) -> Result<u32, ProtocolError> {
        Ok(1)
    }

    async fn session_stop(&self, _session_id: u32) -> Result<(), ProtocolError> {
        Ok(())
    }

    async fn get(
        &self,
        _session_id: u32,
        address: &Address,
    ) -> Result<(Fragment, Bytes), ProtocolError> {
        let first = {
            let mut asked = self.asked.lock().expect("asked");
            asked.push(*address);
            asked
                .iter()
                .filter(|asked| asked.hash == address.hash)
                .count()
                == 1
        };
        if first
            && let Some((hash, hold)) = &self.serving.hold
            && *hash == address.hash
        {
            hold.notified().await;
        }
        if self.serving.slow_down == Some(*address) && !self.slowed.swap(true, Ordering::SeqCst) {
            return Err(ProtocolError::from(lore_base::error::SlowDown));
        }
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(in_flight, Ordering::SeqCst);
        if let Some(gate) = &self.serving.gate {
            gate.wait().await;
        }
        let served = self.serve(*address).await;
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        served
    }

    async fn put(
        &self,
        _session_id: u32,
        _address: Address,
        _fragment: Fragment,
        _payload: Option<Bytes>,
    ) -> Result<(), ProtocolError> {
        Err(ProtocolError::internal("the serving remote takes no puts"))
    }

    async fn query(&self, _session_id: u32, _address: &[Address]) -> Result<Bytes, ProtocolError> {
        Err(ProtocolError::internal(
            "the serving remote takes no queries",
        ))
    }

    async fn verify(
        &self,
        _session_id: u32,
        address: &Address,
        heal: bool,
    ) -> Result<VerifyResult, ProtocolError> {
        if !(heal && self.serving.heals && self.serving.tampered == Some(*address)) {
            return Err(ProtocolError::internal("the serving remote heals nothing"));
        }
        self.healed.store(true, Ordering::SeqCst);
        Ok(VerifyResult {
            corrupted: true,
            healed: HealResult::Healed,
        })
    }

    async fn copy(
        &self,
        _session_id: u32,
        _source_partition: Partition,
        _source_address: Address,
        _target_context: Context,
    ) -> Result<(), ProtocolError> {
        Err(ProtocolError::internal(
            "the serving remote takes no copies",
        ))
    }
}

impl ServingStorage {
    /// What `source` stores at `address`, altered where it is the tampered address and not healed.
    async fn serve(&self, address: Address) -> Result<(Fragment, Bytes), ProtocolError> {
        let stored = self
            .source
            .immutable_store()
            .get(self.source.id, address)
            .await
            .map_err(|err| ProtocolError::internal(err.to_string()))?;
        let payload = stored
            .payload
            .ok_or_else(|| ProtocolError::internal("the source holds no payload"))?;
        if self.serving.tampered == Some(address) && !self.healed.load(Ordering::SeqCst) {
            let mut altered = payload.to_vec();
            altered[0] ^= 0xff;
            return Ok((stored.fragment, Bytes::from(altered)));
        }
        Ok((stored.fragment, payload))
    }

    /// The most gets the remote held at once.
    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    /// The distinct addresses asked of the remote.
    fn asked(&self) -> BTreeSet<Address> {
        self.asked.lock().expect("asked").iter().copied().collect()
    }

    /// How many gets the remote was asked.
    fn gets(&self) -> usize {
        self.asked.lock().expect("asked").len()
    }

    /// How many gets the remote was asked for `address`.
    fn gets_of(&self, address: Address) -> usize {
        self.asked
            .lock()
            .expect("asked")
            .iter()
            .filter(|asked| **asked == address)
            .count()
    }
}

/// Yields until `condition` holds.
async fn until(condition: impl Fn() -> bool) {
    while !condition() {
        tokio::task::yield_now().await;
    }
}

/// A session over a remote serving what `source` stores as `serving` says ([`ServingStorage`]),
/// and that remote.
async fn serving_session(
    source: &Arc<RepositoryContext>,
    serving: Serving,
) -> (Arc<StorageSession>, Arc<ServingStorage>) {
    let storage = Arc::new(ServingStorage {
        source: source.clone(),
        serving,
        asked: std::sync::Mutex::default(),
        healed: AtomicBool::new(false),
        slowed: AtomicBool::new(false),
        in_flight: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
    });
    let connection = Connection::detached(vec![storage.clone()]);
    let session = under_execution_context(|| async {
        connection
            .session(source.id, "cache")
            .await
            .expect("a session over the serving remote")
    })
    .await;
    (session, storage)
}

/// A repository holding 1 MiB of content, unique to `seed`, stored as a fragment list, and that
/// list's address. Read flights join on content across tests, so each test writes its own.
async fn written_list(seed: u8) -> (Arc<RepositoryContext>, Address) {
    let written = context_with_state(RemoteState::Offline).await;
    let content: Vec<u8> = (0..1usize << 20)
        .map(|index| (index.wrapping_mul(2_654_435_761) >> 13) as u8 ^ seed)
        .collect();
    // Boxed: the write pipeline's future sits at the crate's `future-size-threshold`.
    let root = under_execution_context(|| {
        Box::pin(write(
            written.clone(),
            Context::from([0x5au8; 16]),
            Bytes::from(content),
            WriteOptions::default(),
        ))
    })
    .await
    .expect("the content stores locally");
    (written, root)
}

/// The addresses of the fragments the list `written` holds at `root` references.
async fn subfragments(written: &Arc<RepositoryContext>, root: Address) -> BTreeSet<Address> {
    let list = written
        .immutable_store()
        .get(written.id, root)
        .await
        .expect("the list is stored");
    assert!(
        list.fragment.flags & FragmentFlags::PayloadFragmented != 0,
        "the content is fragmented"
    );
    let (_, references) = lore_storage::decompress_and_verify(
        list.fragment,
        list.payload.expect("the list payload"),
        root,
        ReadOptions::default().with_decompress(),
    )
    .expect("the list expands");
    references
        .to_aligned::<FragmentReference>()
        .as_type_slice::<FragmentReference>()
        .iter()
        .map(|reference| Address {
            context: root.context,
            hash: reference.hash,
        })
        .collect()
}

/// Whether `context`'s local store holds a payload for every one of `addresses`.
async fn held(context: &Arc<RepositoryContext>, addresses: &BTreeSet<Address>) -> bool {
    for address in addresses {
        if !is_stored_local(context.clone(), *address).await {
            return false;
        }
    }
    true
}

/// A cache of a fragment list the local store holds without its subfragments asks the remote
/// for exactly those, and stores them; without `cache_fragmented` it asks for nothing.
#[tokio::test]
async fn caching_a_local_list_asks_the_remote_for_its_missing_subfragments() {
    let (written, root) = written_list(0x51).await;
    let leaves = subfragments(&written, root).await;
    let list = written
        .immutable_store()
        .get(written.id, root)
        .await
        .expect("the list is stored");
    let cached = context_with_state(RemoteState::Offline).await;
    cached
        .immutable_store()
        .put(cached.id, root, list.fragment, list.payload, false)
        .await
        .expect("the list alone is stored");

    for cache_fragmented in [false, true] {
        let (session, remote) = serving_session(&written, Serving::default()).await;
        let fetched = under_execution_context(|| {
            cache_through(
                cached.clone(),
                session,
                None,
                vec![root],
                cache_fragmented,
                false,
            )
        })
        .await
        .into_result()
        .expect("the cache fetches what it lacks");
        if cache_fragmented {
            assert_eq!(fetched, leaves.len());
            assert_eq!(
                remote.asked(),
                leaves,
                "the remote was asked for the subfragments"
            );
            assert!(held(&cached, &leaves).await, "the subfragments are stored");
        } else {
            assert_eq!(fetched, 0);
            assert!(
                remote.asked().is_empty(),
                "the remote was asked for nothing"
            );
        }
    }
}

/// A cache of a fragment list the local store lacks fetches it, then the subfragments the
/// fetched list references, and counts the payload bytes it fetched.
#[tokio::test]
async fn caching_a_remote_list_fetches_its_subfragments() {
    let (written, root) = written_list(0x52).await;
    let mut wanted = subfragments(&written, root).await;
    wanted.insert(root);
    let cached = context_with_state(RemoteState::Offline).await;
    let (session, remote) = serving_session(&written, Serving::default()).await;
    let before = lore_storage::remote_fetch_bytes();

    let fetched = under_execution_context(|| {
        cache_through(cached.clone(), session, None, vec![root], true, false)
    })
    .await
    .into_result()
    .expect("the cache fetches what it lacks");

    assert_eq!(fetched, wanted.len());
    assert_eq!(remote.asked(), wanted);
    assert!(
        held(&cached, &wanted).await,
        "the list and its subfragments are stored"
    );
    assert!(
        lore_storage::remote_fetch_bytes() > before,
        "the fetched bytes are counted"
    );
}

/// `LORE_CACHE_GETS` takes a count; zero turns the cap off, and a value that is not a count
/// leaves the default.
#[test]
fn the_cache_get_cap_parses_and_defaults() {
    let cap = |value: &str| cache_gets_from(Some(value.to_string()));
    assert_eq!(cache_gets_from(None), 512, "the measured default");
    assert_eq!(cap(" 128 "), 128);
    assert_eq!(cap("0"), 0);
    assert_eq!(cap("lots"), CACHE_GETS);
    assert_eq!(cap(&usize::MAX.to_string()), usize::MAX);
}

/// A corrupt local list naming its own address is read verified, so it is never walked: the cache
/// asks the remote for an intact copy of it alone, and with none to be had fails.
#[tokio::test]
async fn caching_a_corrupt_local_list_naming_itself_ends() {
    let context = context_with_state(RemoteState::Offline).await;
    let root = Address {
        hash: Hash::from([0x3cu8; 32]),
        context: Context::from([0x5au8; 16]),
    };
    let reference = FragmentReference {
        hash: root.hash,
        offset_content: 0,
    };
    let list = Bytes::copy_from_slice(zerocopy::IntoBytes::as_bytes(&reference));
    let fragment = Fragment {
        flags: FragmentFlags::PayloadFragmented.bits(),
        size_payload: list.len() as u32,
        size_content: 2 * lore_base::types::FRAGMENT_SIZE_THRESHOLD as u64,
    };
    context
        .immutable_store()
        .put(context.id, root, fragment, Some(list), false)
        .await
        .expect("the corrupt list is stored");

    let empty = context_with_state(RemoteState::Offline).await;
    let (session, remote) = serving_session(&empty, Serving::default()).await;
    let cached = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        under_execution_context(|| {
            cache_through(context.clone(), session, None, vec![root], true, false)
        }),
    )
    .await
    .expect("the cache of a list naming itself ends");
    assert!(
        cached.into_result().is_err(),
        "a list with no intact copy fails the cache"
    );
    assert_eq!(remote.asked(), BTreeSet::from([root]));
}

/// A local fragment list that fails its check is fetched again, as a read replaces a corrupt
/// fragment: the intact copy overwrites it, and the subfragments it references are fetched.
#[tokio::test]
async fn caching_a_corrupt_local_list_fetches_and_overwrites_it() {
    let (written, root) = written_list(0x53).await;
    let mut wanted = subfragments(&written, root).await;
    wanted.insert(root);
    let list = written
        .immutable_store()
        .get(written.id, root)
        .await
        .expect("the list is stored");
    let mut corrupt = list.payload.expect("the list payload").to_vec();
    corrupt[0] ^= 0xff;
    let cached = context_with_state(RemoteState::Offline).await;
    cached
        .immutable_store()
        .put(
            cached.id,
            root,
            list.fragment,
            Some(Bytes::from(corrupt)),
            false,
        )
        .await
        .expect("the corrupt list is stored");
    let (session, remote) = serving_session(&written, Serving::default()).await;

    let fetched = under_execution_context(|| {
        cache_through(cached.clone(), session, None, vec![root], true, false)
    })
    .await
    .into_result()
    .expect("the cache fetches what it lacks");

    assert_eq!(fetched, wanted.len());
    assert_eq!(
        remote.asked(),
        wanted,
        "the remote was asked for the list and its subfragments"
    );
    let stored = cached
        .immutable_store()
        .get(cached.id, root)
        .await
        .expect("the list is stored");
    lore_storage::decompress_and_verify(
        stored.fragment,
        stored.payload.expect("the list payload"),
        root,
        ReadOptions::default().with_decompress().with_verify(),
    )
    .expect("the intact list replaced the corrupt one");
    assert!(held(&cached, &wanted).await, "the subfragments are stored");
}

/// `content` stored with its payload in a repository of its own, standing in for the remote's
/// copy, and its address and fragment.
async fn remote_copy(
    content: &'static [u8],
    seed: u8,
) -> (Arc<RepositoryContext>, Address, Fragment) {
    let source = context_with_state(RemoteState::Offline).await;
    let address = Address {
        hash: lore_storage::hash::hash_slice(content),
        context: Context::from([seed; 16]),
    };
    let fragment = Fragment {
        flags: 0,
        size_payload: content.len() as u32,
        size_content: content.len() as u64,
    };
    source
        .immutable_store()
        .put(
            source.id,
            address,
            fragment,
            Some(Bytes::from_static(content)),
            false,
        )
        .await
        .expect("the remote's copy is stored");
    (source, address, fragment)
}

/// An index entry without its payload, as eviction leaves one, holds nothing to serve, so the
/// cache fetches it as a miss and stores the payload.
#[tokio::test]
async fn caching_an_entry_without_its_payload_fetches_it() {
    let (source, address, fragment) =
        remote_copy(b"content whose payload the store does not hold", 0x5a).await;
    let context = context_with_state(RemoteState::Offline).await;
    context
        .immutable_store()
        .put(context.id, address, fragment, None, false)
        .await
        .expect("the entry is stored without its payload");
    let (session, remote) = serving_session(&source, Serving::default()).await;

    let fetched = under_execution_context(|| {
        cache_through(context.clone(), session, None, vec![address], false, false)
    })
    .await
    .into_result()
    .expect("the cache fetches what it lacks");

    assert_eq!(fetched, 1);
    assert_eq!(remote.asked(), BTreeSet::from([address]));
    assert!(
        is_stored_local(context.clone(), address).await,
        "the payload is stored"
    );
}

/// A fetched payload no read follows is stored as fetched, unchecked, as a read checks what it
/// loads: nothing decodes or hashes it, so even one altered on the way is stored.
#[tokio::test]
async fn a_fetched_payload_no_read_follows_is_stored_unchecked() {
    let content = b"content the remote serves altered";
    let (source, address, _) = remote_copy(content, 0x5c).await;
    let cached = context_with_state(RemoteState::Offline).await;
    let (session, _) = serving_session(
        &source,
        Serving {
            tampered: Some(address),
            ..Serving::default()
        },
    )
    .await;
    lore_storage::test_util::reset_counts();

    let fetched = under_execution_context(|| {
        cache_through(cached.clone(), session, None, vec![address], false, false)
    })
    .await
    .into_result()
    .expect("the payload is cached");

    assert_eq!(fetched, 1);
    let (_, decodes, hashes) = lore_storage::test_util::counts();
    assert_eq!((decodes, hashes), (0, 0), "nothing is decoded or hashed");
    let mut altered = content.to_vec();
    altered[0] ^= 0xff;
    let stored = cached
        .immutable_store()
        .get(cached.id, address)
        .await
        .expect("the fragment is stored");
    assert_eq!(
        stored.payload.as_deref(),
        Some(&altered[..]),
        "the payload is stored as fetched"
    );
}

/// A verifying cache checks every payload on receipt, for readers that trust local bytes: one the
/// remote serves altered, and still serves altered after the heal the check asks for, is gone to
/// the remote for but neither counted as fetched bytes nor stored, and the cache answers the
/// failure.
#[tokio::test]
async fn a_verifying_cache_stores_no_payload_that_fails_its_check() {
    let content = b"content the remote serves altered to a verifying cache";
    let (source, address, _) = remote_copy(content, 0x5e).await;
    let cached = context_with_state(RemoteState::Offline).await;
    let (session, _) = serving_session(
        &source,
        Serving {
            tampered: Some(address),
            ..Serving::default()
        },
    )
    .await;

    let outcome = under_execution_context(|| {
        cache_through(cached.clone(), session, None, vec![address], false, true)
    })
    .await;

    assert_eq!((outcome.fragments, outcome.bytes), (1, 0));
    assert!(
        outcome.failure.is_some(),
        "the cache answers the failed check"
    );
    assert!(
        !is_stored_local(cached.clone(), address).await,
        "the altered payload is not stored"
    );
}

/// A cache answers what it came to, checking every payload or not: the fragments it went to the
/// remote for, the payload bytes it fetched of those, and none found local while it waited. It
/// stores what it fetched.
#[tokio::test]
async fn a_cache_counts_the_fragments_and_payload_bytes_it_fetches() {
    for (content, seed, verify) in [
        (
            &b"content whose payload bytes the cache counts"[..],
            0x5f,
            false,
        ),
        (
            &b"content whose payload bytes a verifying cache counts"[..],
            0x6a,
            true,
        ),
    ] {
        let (source, address, fragment) = remote_copy(content, seed).await;
        let cached = context_with_state(RemoteState::Offline).await;
        let (session, _) = serving_session(&source, Serving::default()).await;

        let outcome = under_execution_context(|| {
            cache_through(cached.clone(), session, None, vec![address], false, verify)
        })
        .await;

        assert!(outcome.failure.is_none(), "{:?}", outcome.failure);
        assert_eq!(
            (outcome.fragments, outcome.bytes, outcome.already_local),
            (1, u64::from(fragment.size_payload), 0),
            "verify {verify}"
        );
        assert!(
            is_stored_local(cached.clone(), address).await,
            "verify {verify}"
        );
    }
}

/// Cache get permits bound the gets a cache holds at the remote at once: with two of them, and
/// each get held until a second one is in, the remote holds two at once and never more.
#[tokio::test]
async fn cache_get_permits_bound_the_gets_in_flight() {
    let source = context_with_state(RemoteState::Offline).await;
    let mut addresses = Vec::new();
    for seed in 0..8u8 {
        let content = Bytes::from(vec![seed; 64 + usize::from(seed)]);
        let address = Address {
            hash: lore_storage::hash::hash_slice(&content),
            context: Context::from([0x60u8; 16]),
        };
        let fragment = Fragment {
            flags: 0,
            size_payload: content.len() as u32,
            size_content: content.len() as u64,
        };
        source
            .immutable_store()
            .put(source.id, address, fragment, Some(content), false)
            .await
            .expect("the remote's copy is stored");
        addresses.push(address);
    }
    let cached = context_with_state(RemoteState::Offline).await;
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let (session, remote) = serving_session(
        &source,
        Serving {
            gate: Some(gate),
            ..Serving::default()
        },
    )
    .await;
    let permits = Some(Arc::new(tokio::sync::Semaphore::new(2)));

    let fetched = tokio::time::timeout(
        Duration::from_secs(10),
        under_execution_context(|| {
            cache_through(
                cached.clone(),
                session,
                permits,
                addresses.clone(),
                false,
                false,
            )
        }),
    )
    .await
    .expect("the gets end in pairs")
    .into_result()
    .expect("the cache fetches what it lacks");

    assert_eq!(fetched, addresses.len());
    assert_eq!(remote.peak(), 2, "two gets at once, and never more");
}

/// A get that had to wait for its permit asks the local store first, and skips a fragment a demand
/// read stored meanwhile without asking the remote. One that is to replace a corrupt local copy,
/// which the store holds, asks the remote all the same.
#[tokio::test]
async fn a_get_that_waited_for_its_permit_skips_a_fragment_stored_meanwhile() {
    let (source, address, fragment) =
        remote_copy(b"content a demand read stores meanwhile", 0x5d).await;
    let payload = source
        .immutable_store()
        .get(source.id, address)
        .await
        .expect("the remote's copy")
        .payload;
    for replace in [false, true] {
        let cached = context_with_state(RemoteState::Offline).await;
        let (session, remote) = serving_session(&source, Serving::default()).await;
        let permits = Arc::new(tokio::sync::Semaphore::new(0));
        let mut fetch = Box::pin(under_execution_context(|| {
            fetch_to_cache(
                cached.clone(),
                session,
                Some(permits.clone()),
                address,
                replace,
                false,
            )
        }));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(fetch.as_mut().poll(cx)))
                .await
                .is_pending(),
            "the get waits for a permit"
        );

        cached
            .immutable_store()
            .put(cached.id, address, fragment, payload.clone(), false)
            .await
            .expect("the demand read stores the fragment");
        permits.add_permits(1);

        let fetched = fetch.await;
        if replace {
            assert!(matches!(fetched, Fetch::Fetched { replace: true, .. }));
            assert_eq!(remote.asked(), BTreeSet::from([address]));
        } else {
            assert!(matches!(fetched, Fetch::Local(local) if local == address));
            assert!(
                remote.asked().is_empty(),
                "the remote was asked for nothing"
            );
        }
    }
}

/// A list root a demand read stores alone while the cache's get for it waits for its permit is
/// found local and walked in the next round: the cache fetches the root's subfragments, never the
/// root. The clock is paused, so the demand read's sleep ends only once every other task waits.
#[tokio::test(start_paused = true)]
async fn a_root_stored_while_its_get_waits_is_walked_in_the_next_round() {
    let (written, root) = written_list(0x55).await;
    let leaves = subfragments(&written, root).await;
    let list = written
        .immutable_store()
        .get(written.id, root)
        .await
        .expect("the list is stored");
    let cached = context_with_state(RemoteState::Offline).await;
    let (session, remote) = serving_session(&written, Serving::default()).await;
    let permits = Arc::new(tokio::sync::Semaphore::new(1));
    let taken = permits
        .clone()
        .try_acquire_owned()
        .expect("the test holds the only permit");

    let cache = under_execution_context(|| {
        cache_through(
            cached.clone(),
            session,
            Some(permits.clone()),
            vec![root],
            true,
            false,
        )
    });
    let demand_read = async {
        tokio::time::sleep(Duration::from_millis(1)).await;
        cached
            .immutable_store()
            .put(cached.id, root, list.fragment, list.payload.clone(), false)
            .await
            .expect("the demand read stores the root");
        drop(taken);
    };
    let (fetched, ()) = tokio::join!(cache, demand_read);

    assert_eq!(
        fetched
            .into_result()
            .expect("the cache fetches what it lacks"),
        leaves.len()
    );
    assert_eq!(remote.asked(), leaves, "the root is not asked for");
    assert!(held(&cached, &leaves).await, "the subfragments are stored");
}

/// A cached fragment is stored durable, as the remote holds it, whether or not the remote marks it
/// so, which leaves it for the client store to evict.
#[tokio::test]
async fn a_cached_fragment_is_stored_durable() {
    let (source, address, _) = remote_copy(b"content the remote serves unmarked", 0x5e).await;
    let cached = context_with_state(RemoteState::Offline).await;
    let (session, _) = serving_session(&source, Serving::default()).await;

    under_execution_context(|| {
        cache_through(cached.clone(), session, None, vec![address], false, false)
    })
    .await
    .into_result()
    .expect("the cache fetches what it lacks");

    let stored = cached
        .immutable_store()
        .get_metadata(cached.id, address)
        .await
        .expect("the fragment is stored");
    assert_ne!(
        stored.fragment.flags & FragmentFlags::PayloadStoredDurable,
        0,
        "the cached fragment is durable"
    );
}

/// A fetched fragment list is checked on receipt, as walking it expands it: one that fails is
/// healed by the remote and fetched once more, as a read's fetch is, and the cache stores it intact
/// and walks it.
#[tokio::test]
async fn a_fetched_list_failing_its_check_is_healed_and_stored() {
    let (written, root) = written_list(0x54).await;
    let leaves = subfragments(&written, root).await;
    let cached = context_with_state(RemoteState::Offline).await;
    let (session, remote) = serving_session(
        &written,
        Serving {
            tampered: Some(root),
            heals: true,
            ..Serving::default()
        },
    )
    .await;

    let fetched = under_execution_context(|| {
        cache_through(cached.clone(), session, None, vec![root], true, false)
    })
    .await
    .into_result()
    .expect("the healed list is cached");

    assert_eq!(fetched, leaves.len() + 1);
    let stored = cached
        .immutable_store()
        .get(cached.id, root)
        .await
        .expect("the list is stored");
    lore_storage::decompress_and_verify(
        stored.fragment,
        stored.payload.expect("the list payload"),
        root,
        ReadOptions::default().with_decompress().with_verify(),
    )
    .expect("the list is stored intact");
    assert!(held(&cached, &leaves).await, "its subfragments are stored");
    assert_eq!(
        remote.gets_of(root),
        2,
        "the list is fetched, healed and fetched once more"
    );
}

/// A cache get the remote answers `SlowDown` is retried, and the cache stores what the retry
/// fetches.
#[tokio::test]
async fn a_cache_get_answered_slow_down_is_retried() {
    let (source, address, _) = remote_copy(b"content the remote slows down once", 0x60).await;
    let cached = context_with_state(RemoteState::Offline).await;
    let (session, remote) = serving_session(
        &source,
        Serving {
            slow_down: Some(address),
            ..Serving::default()
        },
    )
    .await;

    let fetched = under_execution_context(|| {
        cache_through(cached.clone(), session, None, vec![address], false, false)
    })
    .await
    .into_result()
    .expect("the retried get is cached");

    assert_eq!(fetched, 1);
    assert_eq!(remote.asked.lock().expect("asked").len(), 2, "asked twice");
    assert!(
        is_stored_local(cached.clone(), address).await,
        "the fragment is stored"
    );
}

/// Identical content under two contexts, cached at once, takes one remote get: the second fetch
/// follows the first's read flight, keyed by the content hash, and both are stored.
#[tokio::test]
async fn identical_content_under_two_contexts_is_fetched_once() {
    let (source, address, fragment) = remote_copy(b"content two files hold alike", 0x61).await;
    let twin = Address {
        hash: address.hash,
        context: Context::from([0x62u8; 16]),
    };
    let payload = source
        .immutable_store()
        .get(source.id, address)
        .await
        .expect("the remote's copy")
        .payload;
    source
        .immutable_store()
        .put(source.id, twin, fragment, payload, false)
        .await
        .expect("the remote's twin is stored");
    let cached = context_with_state(RemoteState::Offline).await;
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let (session, remote) = serving_session(
        &source,
        Serving {
            gate: Some(gate.clone()),
            ..Serving::default()
        },
    )
    .await;

    let followers = lore_storage::test_util::followers();

    let cache = under_execution_context(|| {
        cache_through(
            cached.clone(),
            session,
            None,
            vec![address, twin],
            false,
            false,
        )
    });
    let release = async {
        until(|| lore_storage::test_util::followers() > followers).await;
        gate.wait().await;
    };
    let (fetched, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(cache, release)
    })
    .await
    .expect("the fetches share one get");

    assert_eq!(
        fetched
            .into_result()
            .expect("the cache fetches what it lacks"),
        2
    );
    assert_eq!(remote.gets(), 1, "one get serves both");
    assert!(
        held(&cached, &BTreeSet::from([address, twin])).await,
        "both are stored"
    );
}

/// A cache fetch and a demand read of the same content share one remote get, whichever starts it:
/// the other follows its read flight, and the cache stores the content either way.
#[tokio::test]
async fn a_cache_fetch_and_a_read_of_the_same_content_share_one_get() {
    for (content, seed, cache_leads) in [
        (&b"content a cache fetch leads"[..], 0x63, true),
        (&b"content a demand read leads"[..], 0x64, false),
    ] {
        let source = context_with_state(RemoteState::Offline).await;
        let address = Address {
            hash: lore_storage::hash::hash_slice(content),
            context: Context::from([seed; 16]),
        };
        let fragment = Fragment {
            flags: 0,
            size_payload: content.len() as u32,
            size_content: content.len() as u64,
        };
        source
            .immutable_store()
            .put(
                source.id,
                address,
                fragment,
                Some(Bytes::copy_from_slice(content)),
                false,
            )
            .await
            .expect("the remote's copy is stored");
        let cached = context_with_state(RemoteState::Offline).await;
        let gate = Arc::new(tokio::sync::Barrier::new(2));
        let (session, remote) = serving_session(
            &source,
            Serving {
                gate: Some(gate.clone()),
                ..Serving::default()
            },
        )
        .await;
        let followers = lore_storage::test_util::followers();

        let cache = async {
            if !cache_leads {
                until(|| remote.gets() == 1).await;
            }
            under_execution_context(|| {
                cache_through(
                    cached.clone(),
                    session.clone(),
                    None,
                    vec![address],
                    false,
                    false,
                )
            })
            .await
        };
        let read = async {
            if cache_leads {
                until(|| remote.gets() == 1).await;
            }
            under_execution_context(|| {
                lore_storage::read::load_fragment(
                    cached.immutable_store(),
                    cached.id,
                    address,
                    ReadOptions::default(),
                    Some(session.clone()),
                )
            })
            .await
        };
        let release = async {
            until(|| lore_storage::test_util::followers() > followers).await;
            gate.wait().await;
        };
        let (fetched, read, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(cache, read, release)
        })
        .await
        .expect("the cache fetch and the read share one get");

        assert_eq!(
            fetched
                .into_result()
                .expect("the cache fetches what it lacks"),
            1
        );
        assert_eq!(read.expect("the read is served").1.as_ref(), content);
        assert_eq!(remote.gets(), 1, "one get serves both");
        assert!(
            is_stored_local(cached.clone(), address).await,
            "the cache stores the content"
        );
    }
}

/// A payload a read's join has checked and found altered is healed for the cache, as a read's
/// fetch is: the remote heals it, and the cache stores the intact copy, while the read, given
/// nothing, fetches for itself and is served intact.
#[tokio::test]
async fn a_payload_a_joining_read_finds_altered_is_healed_and_stored() {
    let content = b"content a joining read finds altered";
    let (source, address, _) = remote_copy(content, 0x65).await;
    let cached = context_with_state(RemoteState::Offline).await;
    let hold = Arc::new(tokio::sync::Notify::new());
    let (session, remote) = serving_session(
        &source,
        Serving {
            tampered: Some(address),
            heals: true,
            hold: Some((address.hash, hold.clone())),
            ..Serving::default()
        },
    )
    .await;
    let followers = lore_storage::test_util::followers();

    let cache = under_execution_context(|| {
        cache_through(
            cached.clone(),
            session.clone(),
            None,
            vec![address],
            false,
            false,
        )
    });
    let read = async {
        until(|| remote.gets() == 1).await;
        under_execution_context(|| {
            lore_storage::read::load_fragment(
                cached.immutable_store(),
                cached.id,
                address,
                ReadOptions::default(),
                Some(session.clone()),
            )
        })
        .await
    };
    let release = async {
        until(|| lore_storage::test_util::followers() > followers).await;
        hold.notify_one();
    };
    let (fetched, read, ()) = Box::pin(tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(cache, read, release)
    }))
    .await
    .expect("the cache and the read end");

    assert_eq!(
        fetched.into_result().expect("the healed payload is cached"),
        1
    );
    assert_eq!(read.expect("the read is served").1.as_ref(), content);
    let stored = cached
        .immutable_store()
        .get(cached.id, address)
        .await
        .expect("the fragment is stored");
    assert_eq!(
        stored.payload.as_deref(),
        Some(&content[..]),
        "the intact copy is stored"
    );
}

/// A cache fetch that joins another's flight gives its permit back, as it makes no get of its own:
/// with two permits, the leader's get held and a follower joined, a third fetch takes the
/// follower's permit and its get reaches the remote while the leader's is still held.
#[tokio::test]
async fn a_cache_fetch_joining_another_gives_its_permit_back() {
    let (source, address, fragment) =
        remote_copy(b"content two files share, fetched once", 0x66).await;
    let twin = Address {
        hash: address.hash,
        context: Context::from([0x67u8; 16]),
    };
    let payload = source
        .immutable_store()
        .get(source.id, address)
        .await
        .expect("the remote's copy")
        .payload;
    source
        .immutable_store()
        .put(source.id, twin, fragment, payload, false)
        .await
        .expect("the remote's twin is stored");
    let (other, content) = (0u32..)
        .map(|index| {
            let content = format!("content fetched alongside, {index}").into_bytes();
            let other = Address {
                hash: lore_storage::hash::hash_slice(&content),
                context: Context::from([0x68u8; 16]),
            };
            (other, content)
        })
        .find(|(other, _)| *other > address.max(twin))
        .expect("content sorting after the shared content, so its fetch is spawned last");
    source
        .immutable_store()
        .put(
            source.id,
            other,
            Fragment {
                flags: 0,
                size_payload: content.len() as u32,
                size_content: content.len() as u64,
            },
            Some(Bytes::from(content)),
            false,
        )
        .await
        .expect("the other content is stored");
    let cached = context_with_state(RemoteState::Offline).await;
    let hold = Arc::new(tokio::sync::Notify::new());
    let (session, remote) = serving_session(
        &source,
        Serving {
            hold: Some((address.hash, hold.clone())),
            ..Serving::default()
        },
    )
    .await;
    let permits = Arc::new(tokio::sync::Semaphore::new(2));

    let cache = under_execution_context(|| {
        cache_through(
            cached.clone(),
            session,
            Some(permits),
            vec![address, twin, other],
            false,
            false,
        )
    });
    let release = async {
        until(|| remote.asked().contains(&other)).await;
        hold.notify_one();
    };
    let (fetched, ()) = Box::pin(tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(cache, release)
    }))
    .await
    .expect("the third fetch takes the follower's permit");

    assert_eq!(
        fetched
            .into_result()
            .expect("the cache fetches what it lacks"),
        3
    );
    assert_eq!(remote.gets(), 2, "the shared content is fetched once");
    assert!(
        held(&cached, &BTreeSet::from([address, twin, other])).await,
        "all three are stored"
    );
}
