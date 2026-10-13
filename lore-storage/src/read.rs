// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::cmp::min;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use bytes::BytesMut;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_base::types::KeyType;
use lore_error_set::prelude::*;
use lore_transport::StorageSession;

use crate::compress;
use crate::concurrency::file_count_limit_acquire;
use crate::defragment::DefragmentSink;
use crate::defragment::defragment_pipeline;
use crate::defragment::defragment_pipeline_leaves;
use crate::defragment::read_defragment;
use crate::error::StorageError;
use crate::errors::SlowDown;
use crate::fragment_flags::FragmentFlags;
use crate::hash;
use crate::immutable_store::ImmutableStore;
use crate::immutable_store::StoreError;
use crate::mutable_store::MutableStore;
use crate::options::ReadOptions;
use crate::store_types::PayloadRead;
use crate::store_types::StoreGetData;
use crate::types::Address;
use crate::types::Fragment;
use crate::types::Partition;

/// Load a single raw fragment from store with retry backoff. How widely the store searches for it
/// is the store's own business - see [`ImmutableStore::read_scope`].
///
/// `verified_by_caller` states that the payload is hashed once this returns, which reports
/// corruption as an error to heal from and leaves the debug check here nothing to add.
pub async fn read_raw(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    verified_by_caller: bool,
) -> Result<(Fragment, Bytes), StorageError> {
    let mut retry = crate::store_retry();
    loop {
        debug_assert!(
            !address.hash.is_zero(),
            "Cannot request zero hash from store"
        );
        match store
            .clone()
            .get(partition, address)
            .await
            .and_then(StoreGetData::into_payload)
        {
            Ok((fragment, payload)) => {
                debug_assert!(
                    verified_by_caller
                        || match hash::hash_fragment(fragment, payload.as_ref()) {
                            Ok(loaded_hash) => loaded_hash == address.hash,
                            Err(_) => true,
                        },
                    "Local store loaded data failed hash validation"
                );
                return Ok((fragment, payload));
            }
            Err(StoreError::SlowDown(_)) => {
                if !retry.wait().await {
                    return Err(StorageError::from(SlowDown));
                }
            }
            Err(StoreError::AddressNotFound(_) | StoreError::PayloadNotFound(_)) => {
                return Err(StorageError::from(crate::errors::AddressNotFound::from(
                    address,
                )));
            }
            Err(err) => {
                return Err(StorageError::internal_with_context(err, "store get failed"));
            }
        }
    }
}

/// Expands `buffer` when `options.decompress` is set and hashes its content against `address` when
/// `options.verify` is set, returning the fragment and payload expanded only if asked.
pub fn decompress_and_verify(
    fragment: Fragment,
    buffer: Bytes,
    address: Address,
    options: ReadOptions,
) -> Result<(Fragment, Bytes), StorageError> {
    if !options.decompress && !options.verify {
        return Ok((fragment, buffer));
    }

    let mut fragment = fragment;
    let mut buffer = buffer;

    let mut content_hash = address.hash;
    // Compressed is a group flag, check if any of the flags are set
    if (fragment.flags & FragmentFlags::PayloadCompressed) != 0 {
        let (decompressed_fragment, decompressed_buffer) =
            compress::decompress(fragment, buffer.as_ref())
                .forward::<StorageError>("failed to decompress fragment")?;
        if options.verify {
            content_hash = hash::hash_slice(decompressed_buffer.as_ref());
        }
        if options.decompress {
            buffer = decompressed_buffer.freeze();
            fragment = decompressed_fragment;
        }
    } else if options.verify {
        content_hash = hash::hash_slice(buffer.as_ref());
    }

    if options.verify && content_hash != address.hash {
        Err(StorageError::internal(format!(
            "fragment hash mismatch, got {content_hash}"
        )))
    } else {
        Ok((fragment, buffer))
    }
}

/// Process-wide count of remote fetches in flight across every [`remote_get_retry`] path; shared by all concurrent operations, layer per-op attribution on top if needed.
pub static REMOTE_FETCH_INFLIGHT: AtomicU64 = AtomicU64::new(0);

/// See [`REMOTE_FETCH_INFLIGHT`].
pub fn remote_fetch_inflight() -> u64 {
    REMOTE_FETCH_INFLIGHT.load(Ordering::Relaxed)
}

/// Process-wide count of payload bytes fetched from a remote, by every [`load_fragment`] path and
/// every caller that fetches through a session directly and adds what it received.
pub static REMOTE_FETCH_BYTES: AtomicU64 = AtomicU64::new(0);

/// See [`REMOTE_FETCH_BYTES`].
pub fn remote_fetch_bytes() -> u64 {
    REMOTE_FETCH_BYTES.load(Ordering::Relaxed)
}

/// RAII guard around [`REMOTE_FETCH_INFLIGHT`] so the counter can't leak on panic or early return.
struct RemoteFetchGuard;
impl RemoteFetchGuard {
    fn new() -> Self {
        REMOTE_FETCH_INFLIGHT.fetch_add(1, Ordering::Relaxed);
        Self
    }
}
impl Drop for RemoteFetchGuard {
    fn drop(&mut self) {
        REMOTE_FETCH_INFLIGHT.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Fetch a fragment from a remote session with retry on `SlowDown` and on
/// transient `NotConnected` responses (e.g. the server's session-id map was
/// reset by a QUIC reconnect; the storage layer mapping turns this into
/// `StorageError::NotConnected`, which we recover from by invalidating the
/// cached session and retrying with a fresh `session_start`).
///
/// `Disconnected` is deliberately not retried here: on both transports it means the
/// transport already exhausted its own reconnect-and-reissue and gave up, so the remote
/// is down rather than the session being stale.
async fn remote_get_retry(
    session: &StorageSession,
    address: Address,
    priority: bool,
) -> Result<(Fragment, Bytes), StorageError> {
    let _guard = RemoteFetchGuard::new();
    #[cfg(feature = "test-util")]
    if let Some(served) = crate::test_util::remote_payload(address) {
        crate::test_util::remote_gate().await;
        return Ok(served);
    }
    let mut retry = crate::store_retry();
    let mut stale_session_retries: u32 = 0;
    loop {
        debug_assert!(
            !address.hash.is_zero(),
            "Cannot request zero hash from store"
        );
        let result = if priority {
            session.get_priority(&address).await
        } else {
            session.get(&address).await
        };
        match result {
            Ok((fragment, payload)) => return Ok((fragment, payload)),
            Err(ref e) if e.is_slow_down() => {
                if !retry.wait().await {
                    return Err(StorageError::from(SlowDown));
                }
            }
            Err(err) => {
                let storage_err = crate::error::protocol_error_to_storage(err, address);
                if matches!(storage_err, StorageError::NotConnected(_))
                    && stale_session_retries < MAX_STALE_SESSION_RETRIES
                {
                    stale_session_retries += 1;
                    session.invalidate().await;
                    if !retry.wait().await {
                        return Err(storage_err);
                    }
                    continue;
                }
                return Err(storage_err);
            }
        }
    }
}

/// Bound on retries for `StorageError::NotConnected` in `remote_get_retry`.
/// Picked so a genuinely permanent server-side failure surfaces quickly
/// rather than looping through the full `store_retry` backoff schedule (60
/// attempts up to 10 s apart). Recovery from a QUIC reconnect typically
/// succeeds on the first or second retry once the session has been
/// re-established.
const MAX_STALE_SESSION_RETRIES: u32 = 5;

/// Outcome of probing the local store for a fragment, distinguishing a plain miss
/// (nothing to report, fall through to remote) from data that was found but failed
/// verification (the refetched payload overwrites the local copy), from a durable payload this
/// build no longer decodes (also refetched and overwritten), and from a fatal error that must
/// short-circuit the caller immediately rather than fall through.
enum LocalFailure {
    Corrupt,
    /// A durably stored payload in an encoding this build no longer decodes, which the remote
    /// holds re-encoded. Carries the decode error to report when there is no remote to fetch
    /// from.
    DurableMigrated(StorageError),
    Other,
    Fatal(StorageError),
}

/// Probe the local store for `address`: read, decompress, and verify. A caller disables it via
/// `options.local`: a remote-only handle, or a read that has probed already.
async fn probe_local(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    options: ReadOptions,
) -> Result<(Fragment, Bytes), LocalFailure> {
    if !options.local {
        return Err(LocalFailure::Other);
    }

    match read_raw(store, partition, address, options.verify).await {
        Ok((fragment, buffer)) => {
            let durable_oodle = (fragment.flags & FragmentFlags::PayloadCompressedOodle2) != 0
                && (fragment.flags & FragmentFlags::PayloadStoredDurable) != 0;
            match decompress_and_verify(fragment, buffer, address, options) {
                Ok((fragment, buffer)) => Ok((fragment, buffer)),
                // The remote re-encodes Oodle on ingress, so it holds a durable payload in a
                // form this build decodes. A local-only one has no such copy to fall back on.
                Err(err) if durable_oodle && matches!(err, StorageError::NotSupported(_)) => {
                    Err(LocalFailure::DurableMigrated(err))
                }
                Err(err) if matches!(err, StorageError::NotSupported(_)) => {
                    Err(LocalFailure::Fatal(err))
                }
                Err(err) => {
                    lore_base::lore_debug!(
                        "Fragment {} failed decompression/verification: {err}",
                        address.hash
                    );
                    debug_assert!(
                        false,
                        "Local store data failed decompression or verification"
                    );
                    Err(LocalFailure::Corrupt)
                }
            }
        }
        Err(e) => {
            lore_base::lore_trace!(
                "Fragment {} failed loading from local store: {e:?}",
                address.hash
            );
            Err(LocalFailure::Other)
        }
    }
}

/// The count of stores landed in `address`'s bucket, read before a local probe, for
/// [`stored_since`]; `None` where the load waits for no store, its local probe being off or
/// `options.never_wait` set. A hit needs no wait: a store publishes its entry only once the
/// payload is written, and the address names the content.
fn landed_before_probe(address: Address, options: ReadOptions) -> Option<u64> {
    (options.local && !options.never_wait).then(|| crate::write::stores_landed(address))
}

/// Whether a store of `address` may have written its payload since the count `landed` was read
/// ([`landed_before_probe`]): a store in flight is waited for, and one waited on or one landed
/// since may have written it, so only then is the local store worth probing again.
async fn stored_since(partition: Partition, address: Address, landed: Option<u64>) -> bool {
    let Some(landed) = landed else {
        return false;
    };
    crate::write::wait_if_in_flight(partition, address).await
        || crate::write::stores_landed(address) != landed
}

/// Checks a remote payload on receipt, its sizes against its length and its content against
/// `address`, and returns that content expanded. A payload that is neither compressed nor a
/// fragment list is the content, so its content size must equal its payload size.
pub fn verify_receipt(
    fragment: Fragment,
    buffer: Bytes,
    address: Address,
) -> Result<(Fragment, Bytes), StorageError> {
    crate::validate_fragment_payload(&fragment, buffer.len())
        .and_then(|()| crate::immutable_store::validate_plain_payload_size(&fragment))
        .forward::<StorageError>("remote fragment sizes disagree with its payload")?;
    let receipt = ReadOptions::default().with_decompress().with_verify();
    decompress_and_verify(fragment, buffer, address, receipt)
}

/// Fetches `address` from the remote through [`fetch_remote`], and stores it locally where
/// `options.cache`, `local_replace` or the fragment's local cache priority asks. A read
/// single-flight leader runs it for itself and its followers, and a follower whose leader failed on
/// another address runs it for itself; it answers the flight's form, so the flight awaits its
/// future directly rather than through a wrapper holding a second copy.
async fn fetch_and_store(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    options: ReadOptions,
    session: Arc<StorageSession>,
    local_replace: bool,
) -> Result<Flown, StorageError> {
    let fetched = fetch_remote(session.as_ref(), address, options.priority).await?;
    let (fragment, payload) = &fetched.raw;
    if options.cache
        || local_replace
        || (fragment.flags & FragmentFlags::PayloadLocalCachePriority) != 0
    {
        let _ = store
            .clone()
            .put(
                partition,
                address,
                *fragment,
                Some(payload.clone()),
                local_replace,
            )
            .await;
    }
    Ok(Flown::Fetched(fetched))
}

/// Fetches `address` from the remote, retrying as [`remote_get_retry`] does, marks it durable, as
/// the remote holds it, and counts its payload bytes ([`remote_fetch_bytes`]): the payload as
/// fetched, unchecked.
pub async fn fetch_payload(
    session: &StorageSession,
    address: Address,
    priority: bool,
) -> Result<(Fragment, Bytes), StorageError> {
    let (mut fragment, buffer) = remote_get_retry(session, address, priority).await?;
    REMOTE_FETCH_BYTES.fetch_add(buffer.len() as u64, Ordering::Relaxed);
    fragment.flags |= FragmentFlags::PayloadStoredDurable;
    Ok((fragment, buffer))
}

/// Fetches `address` as [`fetch_payload`] does, and checks it as [`check_fetched`] does.
pub async fn fetch_remote(
    session: &StorageSession,
    address: Address,
    priority: bool,
) -> Result<Fetched, StorageError> {
    lore_base::lore_trace!("Fetch immutable fragment {} from remote", address);
    let raw = fetch_payload(session, address, priority).await?;
    check_fetched(session, address, priority, raw).await
}

/// Checks `raw`, the payload at `address` as fetched ([`fetch_payload`]), on receipt
/// ([`verify_receipt`]): its sizes against its length, and its content against the address. A
/// payload that fails the check is asked of the server to heal once, and fetched again. The content
/// the check expands is handed back, so no caller decodes or hashes a fetched payload again.
pub async fn check_fetched(
    session: &StorageSession,
    address: Address,
    priority: bool,
    mut raw: (Fragment, Bytes),
) -> Result<Fetched, StorageError> {
    let mut heal_attempted = false;
    loop {
        match verify_receipt(raw.0, raw.1.clone(), address) {
            Ok(content) => return Ok(Fetched { content, raw }),
            Err(err) => {
                if matches!(err, StorageError::NotSupported(_)) {
                    return Err(err);
                }
                if heal_attempted {
                    lore_base::lore_error!(
                        "Fragment {} still corrupt after heal: {}",
                        address.hash,
                        err
                    );
                    return Err(err);
                }

                lore_base::lore_warn!("Fragment {}: {}. Attempting heal.", address.hash, err);

                let healed = session
                    .verify(&address, true)
                    .await
                    .is_ok_and(|r| r.healed == lore_base::types::HealResult::Healed);

                if !healed {
                    lore_base::lore_error!("Server did not heal fragment {}", address.hash);
                    return Err(err);
                }

                lore_base::lore_debug!("Server healed fragment {}, retrying fetch", address.hash);
                heal_attempted = true;
                raw = fetch_payload(session, address, priority).await?;
            }
        }
    }
}

/// Unified fragment load: local -> decompress/verify -> optional remote fallback -> heal -> cache.
///
/// When `remote_session` is `Some`, the session is used for remote fetch if the
/// local load fails (miss or corrupt). If the remote data fails verification,
/// heal is attempted once via `session.verify()` before retrying.
///
/// For local-only loading, pass `None`.
///
/// Concurrent misses of the same content join a read single-flight
/// (`read_flight::join_read_flight`), keyed by the content hash, and by the partition where the
/// store isolates partitions, never by the context: the first caller becomes the leader and runs
/// `fetch_and_store`, and later callers await its outcome rather than duplicating
/// the remote fetch. A leader that vanishes without completing (e.g. its task is
/// cancelled) leaves no result behind; the next follower observing that promotes
/// itself to leader and retries. A local-only load (no session, or `options.remote`
/// off) answers its miss with `AddressNotFound` and never joins: following would wait
/// on a remote fetch it may not make. With `options.never_wait` a miss answers
/// `AddressNotFound` at once, waiting neither for a store in flight nor a flight.
pub async fn load_fragment(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    options: ReadOptions,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<(Fragment, Bytes), StorageError> {
    if address.hash.is_zero() {
        return Ok((Fragment::default(), Bytes::default()));
    }

    let landed = landed_before_probe(address, options);
    let mut probed = probe_local(store.clone(), partition, address, options).await;
    if matches!(probed, Err(LocalFailure::Corrupt | LocalFailure::Other))
        && stored_since(partition, address, landed).await
    {
        probed = probe_local(store.clone(), partition, address, options).await;
    }
    // The failure is consumed here rather than kept, so the future does not carry it across the
    // fetch. A corrupt or undecodable local entry has to be overwritten, not deduplicated against.
    let remote = remote_session.filter(|_| options.remote && !options.never_wait);
    let (session, local_replace) = match (remote, probed) {
        (_, Ok((fragment, payload))) => return Ok((fragment, payload)),
        // The payload is held but cannot be decoded, which a miss would misreport.
        (_, Err(LocalFailure::Fatal(err))) | (None, Err(LocalFailure::DurableMigrated(err))) => {
            return Err(err);
        }
        (Some(session), Err(LocalFailure::Corrupt | LocalFailure::DurableMigrated(_))) => {
            (session, true)
        }
        (Some(session), Err(LocalFailure::Other)) => (session, false),
        (None, Err(LocalFailure::Corrupt | LocalFailure::Other)) => {
            return Err(StorageError::from(crate::errors::AddressNotFound::from(
                address,
            )));
        }
    };

    fetch_single_flight(
        store.clone(),
        partition,
        address,
        options,
        || {
            fetch_and_store(
                store.clone(),
                partition,
                address,
                options,
                session.clone(),
                local_replace,
            )
        },
        || {},
    )
    .await
    .map(|flown| flown.take(options.decompress))
}

/// A remote fetch's result ([`fetch_remote`]).
#[derive(Clone, Debug)]
pub struct Fetched {
    /// The content, expanded and verified on receipt, and the fragment describing it.
    pub content: (Fragment, Bytes),
    /// The payload as fetched, and its fragment, marked durable: the stored form.
    pub raw: (Fragment, Bytes),
}

impl Fetched {
    /// The content when `decompress`, the payload as fetched otherwise.
    fn take(self, decompress: bool) -> (Fragment, Bytes) {
        if decompress { self.content } else { self.raw }
    }
}

/// What a fetch in a read flight came to ([`fetch_single_flight`]).
#[derive(Debug)]
pub enum Flown {
    /// A fetch checked on receipt, the caller's own or the one it followed.
    Fetched(Fetched),
    /// The payload as fetched and marked durable, unchecked ([`fetch_payload`]): a fetch of the
    /// caller's own that no follower joined, so nothing asked for it to be checked.
    Unverified((Fragment, Bytes)),
    /// The payload as fetched, which failed the check a follower's join asked for: nothing was
    /// published, so the followers fetch for themselves.
    Rejected((Fragment, Bytes)),
    /// The local store's copy, in the form `options` asks, found after the leader failed on another
    /// address.
    Local((Fragment, Bytes)),
}

impl Flown {
    /// The content when `decompress`, the payload as fetched otherwise, the payload unchecked or
    /// rejected, or the local copy as found.
    pub fn take(self, decompress: bool) -> (Fragment, Bytes) {
        match self {
            Self::Fetched(fetched) => fetched.take(decompress),
            Self::Unverified(raw) | Self::Rejected(raw) => raw,
            Self::Local(found) => found,
        }
    }
}

/// Runs `fetch` for concurrent callers missing the content `address` names: the first becomes the
/// leader and fetches, and the rest follow its outcome, whatever their context, and whatever their
/// partition where the store does not isolate partitions. A follower takes the leader's error only
/// where it asked for the leader's own partition and address; one whose leader failed on another
/// address probes the local store, unless `options.local` is off, and fetches itself when nothing
/// is there.
///
/// Followers share the leader's fetch rather than copy it, and the leader hands it over only when
/// a follower is subscribed. A leader whose `fetch` answers a payload unchecked checks it only
/// then ([`ReadFlightGuard::complete_unverified`](crate::read_flight::ReadFlightGuard::complete_unverified)).
/// `on_follow` runs when the caller first joins as a follower, before it waits, so a caller holding
/// what only its own fetch needs gives it back there.
pub async fn fetch_single_flight<F, Fut, G>(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    options: ReadOptions,
    fetch: F,
    on_follow: G,
) -> Result<Flown, StorageError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<Flown, StorageError>>,
    G: FnOnce(),
{
    let isolate_partitions = store.isolates_partitions();
    let mut on_follow = Some(on_follow);
    loop {
        match crate::read_flight::join_read_flight(partition, address, isolate_partitions) {
            crate::read_flight::ReadFlight::Leader(guard) => {
                let result = match fetch().await {
                    Ok(Flown::Unverified(raw)) => {
                        return Ok(guard.complete_unverified(address, raw));
                    }
                    Ok(Flown::Fetched(fetched)) => Ok(fetched),
                    Ok(local) => return Ok(local),
                    Err(err) => Err(err),
                };
                guard.complete(&result);
                return result.map(Flown::Fetched);
            }
            crate::read_flight::ReadFlight::Follower(follower) => {
                if let Some(on_follow) = on_follow.take() {
                    on_follow();
                }
                let own = follower.led_by(partition, address);
                match crate::read_flight::await_flight(follower).await {
                    Some(Ok(fetched)) => return Ok(Flown::Fetched(fetched)),
                    Some(Err(err)) if own => return Err(err),
                    Some(Err(_)) => {
                        if let Ok(found) =
                            probe_local(store.clone(), partition, address, options).await
                        {
                            return Ok(Flown::Local(found));
                        }
                        return fetch().await;
                    }
                    None => {}
                }
            }
        }
    }
}

/// Load a single raw fragment from local store, optionally decompressing and verifying.
/// Does not reassemble fragmented data or fallback to remote.
/// Thin wrapper around [`load_fragment`] with no remote session.
pub async fn load_raw_local(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    options: ReadOptions,
) -> Result<(Fragment, Bytes), StorageError> {
    load_fragment(store, partition, address, options, None).await
}

/// Resolve a caller's content range against the content that actually exists.
///
/// `None` is the whole content. A range reaching past the end is clamped rather than refused:
/// a caller reading the tail of content whose size it holds from an earlier lookup gets the
/// bytes that are there. A start past the end resolves to empty — callers that need to tell
/// that apart from genuinely empty content compare their own start against `size_content`,
/// which every entry point here reports back alongside the bytes.
///
/// The result is never inverted, whatever the caller passed, so it is safe to hand to
/// [`Bytes::slice`], which panics on a range starting past its own end.
pub fn resolve_content_range(range: Option<Range<usize>>, size_content: u64) -> Range<usize> {
    let end = usize::try_from(size_content).unwrap_or(usize::MAX);
    match range {
        Some(range) => {
            let start = min(range.start, end);
            start..min(range.end, end).max(start)
        }
        None => 0..end,
    }
}

/// Read content (defragmenting if needed) into a `Bytes` buffer, returning the fragment
/// describing the whole content alongside the bytes the range asked for.
///
/// The fragment comes back because the bytes alone no longer say how much content there is:
/// with a range, `size_content` is what exists and the buffer length is what was asked for.
pub async fn read(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    range: Option<Range<usize>>,
    options: ReadOptions,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<(Fragment, Bytes), StorageError> {
    let options = options.with_decompress();
    let (fragment, buffer) = load_fragment(
        store.clone(),
        partition,
        address,
        options,
        remote_session.clone(),
    )
    .await?;

    if let Some(max) = options.max_content_size
        && fragment.size_content > max
    {
        return Err(StorageError::from(crate::errors::Oversized {
            context: format!(
                "fragment size_content {} exceeds caller-supplied max {max}",
                fragment.size_content
            ),
        }));
    }

    let range = resolve_content_range(range, fragment.size_content);
    if range.is_empty() {
        return Ok((fragment, Bytes::default()));
    }

    if (fragment.flags & FragmentFlags::PayloadFragmented) == FragmentFlags::PayloadFragmented {
        let mut target_buffer = BytesMut::with_capacity(range.len());
        unsafe {
            target_buffer.set_len(range.len());
        }
        let target_size = target_buffer.len();
        let target = target_buffer.split();
        read_defragment(
            store,
            partition,
            address,
            range,
            fragment,
            buffer,
            target,
            options,
            0,
            remote_session,
        )
        .await?;
        if !target_buffer.try_reclaim(target_size) {
            return Err(StorageError::internal(
                "failed to reclaim buffer after defragmenting",
            ));
        }
        unsafe {
            target_buffer.set_len(target_size);
        }
        Ok((fragment, target_buffer.freeze()))
    } else {
        Ok((fragment, buffer.slice(range)))
    }
}

/// Read content into a pre-allocated buffer with offset/length, verifying it when `options.verify`
/// is set.
///
/// A whole read of one compressed fragment the local store holds expands into `slice` and is
/// verified there, allocating nothing of the content's size; any other payload the local store
/// holds is expanded and verified once before its range is copied in. A payload the local store
/// cannot read is loaded through [`load_fragment`], which reads it locally where a store of it may
/// have landed meanwhile ([`stored_since`]) and fetches it from the remote otherwise, and one that
/// fails to expand or verify is loaded through it again, which replaces the local copy from the
/// remote. That load is boxed, as only a cache miss or a corrupt payload takes it.
///
/// The contents of `slice` are unspecified when this fails.
pub async fn read_into(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    range: Option<Range<usize>>,
    slice: &mut [u8],
    options: ReadOptions,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<(), StorageError> {
    let (fragment, buffer) = {
        let probes = options.local && !address.hash.is_zero();
        let landed = landed_before_probe(address, options);
        let stored = if probes {
            read_raw(store.clone(), partition, address, options.verify)
                .await
                .ok()
        } else {
            None
        };

        if let Some((fragment, buffer)) = &stored
            && slice.len() as u64 == fragment.size_content
            && resolve_content_range(range.clone(), fragment.size_content).len() == slice.len()
            && options
                .max_content_size
                .is_none_or(|max| fragment.size_content <= max)
            && (fragment.flags & FragmentFlags::PayloadFragmented)
                != FragmentFlags::PayloadFragmented
            && (fragment.flags & FragmentFlags::PayloadCompressed) != 0
            && compress::decompress_into_slice(*fragment, buffer, slice).is_ok()
            && (!options.verify || hash::hash_slice(slice) == address.hash)
        {
            return Ok(());
        }

        match stored.map(|(fragment, buffer)| {
            decompress_and_verify(fragment, buffer, address, options.with_decompress())
        }) {
            Some(Ok(content)) => content,
            loaded => {
                let reprobe =
                    loaded.is_some() || (probes && stored_since(partition, address, landed).await);
                let options = if reprobe {
                    options.with_decompress()
                } else {
                    options.with_decompress().no_local()
                };
                Box::pin(load_fragment(
                    store.clone(),
                    partition,
                    address,
                    options,
                    remote_session.clone(),
                ))
                .await?
            }
        }
    };

    if let Some(max) = options.max_content_size
        && fragment.size_content > max
    {
        return Err(StorageError::from(crate::errors::Oversized {
            context: format!(
                "fragment size_content {} exceeds caller-supplied max {max}",
                fragment.size_content
            ),
        }));
    }

    let range = resolve_content_range(range, fragment.size_content);
    if range.is_empty() {
        return Ok(());
    }
    if slice.len() != range.len() {
        return Err(StorageError::internal(format!(
            "unexpected size: slice {} vs range {}",
            slice.len(),
            range.len()
        )));
    }

    if (fragment.flags & FragmentFlags::PayloadFragmented) == FragmentFlags::PayloadFragmented {
        let content_size = range.len();
        let mut content = BytesMut::with_capacity(content_size);
        unsafe {
            content.set_len(content_size);
        }
        let target = content.split();
        read_defragment(
            store,
            partition,
            address,
            range,
            fragment,
            buffer,
            target,
            options.with_decompress(),
            0,
            remote_session,
        )
        .await?;
        if !content.try_reclaim(content_size) {
            return Err(StorageError::internal(
                "failed to reclaim buffer after defragmenting",
            ));
        }
        unsafe {
            content.set_len(content_size);
        }
        if slice.len() != content.len() {
            return Err(StorageError::internal(format!(
                "unexpected size: slice {} vs content {}",
                slice.len(),
                content.len()
            )));
        }
        slice.copy_from_slice(content.as_ref());
    } else {
        let buffer = buffer.slice(range);
        if slice.len() != buffer.len() {
            return Err(StorageError::internal(format!(
                "unexpected size: slice {} vs buffer {}",
                slice.len(),
                buffer.len()
            )));
        }
        slice.copy_from_slice(buffer.as_ref());
    }
    Ok(())
}

/// Whether the `size` bytes of content sitting in `dst` hash to the address they were read from.
///
/// A `dst` holding fewer than `size` bytes fails rather than being read past: the content it holds
/// is not the content the address names either way.
fn content_verifies(size: usize, address: Address, dst: &mut crate::CallerBuffer) -> bool {
    let Some(content) = dst.as_mut_slice().get_mut(..size) else {
        lore_base::lore_debug!(
            "Fragment {} claims {size} bytes of content, more than its destination holds",
            address.hash
        );
        return false;
    };

    let content_hash = hash::hash_slice(content);
    if content_hash != address.hash {
        lore_base::lore_debug!(
            "Fragment {} failed verification in caller buffer, got {content_hash}",
            address.hash
        );
        return false;
    }
    true
}

/// Copy `bytes` into the start of `dst`, refusing content the destination has no room for rather
/// than truncating it.
fn copy_into_buffer(bytes: &[u8], dst: &mut crate::CallerBuffer) -> Result<usize, StorageError> {
    let capacity = dst.len();
    let Some(target) = dst.as_mut_slice().get_mut(..bytes.len()) else {
        return Err(StorageError::from(crate::errors::Oversized {
            context: format!(
                "content of {} bytes exceeds the {capacity} byte destination buffer",
                bytes.len()
            ),
        }));
    };
    target.copy_from_slice(bytes);
    Ok(bytes.len())
}

/// Whether the remote may supply the root of a read into a caller-owned buffer.
///
/// An address the caller named is authoritative, so the remote may serve it. A hash a local mutable
/// mapping resolved to is not: the mapping is trusted as it stands and may name content the key has
/// since moved off, so reading that hash from the remote would answer with content the key no
/// longer names. [`read_resolved`] re-resolves against the remote instead, which costs the same
/// round trip and answers against the authoritative mapping.
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum RootSource {
    LocalOrRemote,
    LocalOnly,
}

/// Where [`content_payload_for_buffer`] left the payload stored under an address.
enum BufferPayload {
    /// `dst` holds the content.
    Placed,
    /// The payload as the local store holds it, still to be turned into content.
    Stored(Bytes),
    /// Content [`load_fragment`] expanded: read locally once a store in flight landed, or fetched
    /// and verified on receipt.
    Fetched(Bytes),
}

/// The payload stored under `address`, put in `dst` when the local store could place it there,
/// and handed back otherwise: as the local store holds it, or as content [`load_fragment`]
/// expanded. With a remote in reach, a local miss waits for a store of the address in flight, as
/// [`stored_since`] describes, and probes the local store again through [`load_fragment`] only
/// where one may have landed. `None` when the local store does not hold it and no remote is in
/// reach; a remote fetch that fails is an error, since it already verified and healed.
async fn content_payload_for_buffer(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    dst: &mut crate::CallerBuffer,
    options: ReadOptions,
    root: RootSource,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<Option<(Fragment, BufferPayload)>, StorageError> {
    let landed = landed_before_probe(address, options);
    if options.local {
        match store.clone().get_into(partition, address, dst).await {
            Ok((fragment, PayloadRead::IntoBuffer)) => {
                return Ok(Some((fragment, BufferPayload::Placed)));
            }
            Ok((fragment, PayloadRead::Returned(payload))) => {
                return Ok(Some((fragment, BufferPayload::Stored(payload))));
            }
            // Forwarded rather than rebuilt: rebuilding put the old error's whole `Display` in
            // the new one's context, so the caller read "Oversized: Oversized: ...".
            Err(err) if err.is_oversized() => {
                return Err(err).forward("reading the content into the caller's buffer");
            }
            Err(err) => {
                lore_base::lore_trace!(
                    "Fragment {} failed loading into caller buffer: {err:?}",
                    address.hash
                );
            }
        }
    }

    // No remote in reach: the caller's fallback loads the root through `load_fragment`, which
    // waits for a store in flight and probes again.
    if root == RootSource::LocalOnly || !options.remote || remote_session.is_none() {
        return Ok(None);
    }

    let options = if stored_since(partition, address, landed).await {
        options.with_decompress()
    } else {
        options.with_decompress().no_local()
    };
    let (fragment, content) =
        load_fragment(store, partition, address, options, remote_session).await?;
    Ok(Some((fragment, BufferPayload::Fetched(content))))
}

/// Read the whole content stored under `address` into `dst`, reporting the fragment describing it
/// and the number of bytes written.
///
/// A local payload lands where it belongs: one that is the content is read into `dst`, a
/// compressed one expands into `dst`, and a fragment list is the root the walk starts from, its
/// leaves written into `dst` in place. Remote content arrives expanded and verified on receipt and
/// is copied into `dst` once. No payload is read twice.
///
/// `None` leaves the read to the assembling path, which loads the root itself and so verifies and
/// heals: the local store did not hold the content and no remote is in reach, what the local store
/// held did not survive expansion or verification, or what it held is larger than
/// `options.max_content_size` allows, which that path reports. A remote fetch that fails, fetched
/// content larger than `options.max_content_size` allows, and a destination too small for the
/// content, fail the read.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_content_into_buffer(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    dst: &mut crate::CallerBuffer,
    options: ReadOptions,
    root: RootSource,
    depth: usize,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<Option<(Fragment, usize)>, StorageError> {
    let options = options.with_decompress();
    let Some((fragment, payload)) = content_payload_for_buffer(
        store.clone(),
        partition,
        address,
        dst,
        options,
        root,
        remote_session.clone(),
    )
    .await?
    else {
        return Ok(None);
    };

    if let Some(max) = options.max_content_size
        && fragment.size_content > max
    {
        if matches!(payload, BufferPayload::Fetched(_)) {
            return Err(StorageError::from(crate::errors::Oversized {
                context: format!(
                    "fragment size_content {} exceeds caller-supplied max {max}",
                    fragment.size_content
                ),
            }));
        }
        return Ok(None);
    }

    let size_content = fragment.size_content as usize;
    let payload = match payload {
        BufferPayload::Placed => {
            if options.verify && !content_verifies(size_content, address, dst) {
                return Ok(None);
            }
            return Ok(Some((fragment, size_content)));
        }
        BufferPayload::Fetched(content) => {
            let written = deliver_root_into_buffer(
                store,
                partition,
                address,
                None,
                fragment,
                content,
                dst,
                options,
                depth,
                remote_session,
            )
            .await?;
            return Ok(Some((fragment, written)));
        }
        BufferPayload::Stored(payload) => payload,
    };

    if (fragment.flags & FragmentFlags::PayloadCompressed) != 0 {
        let capacity = dst.len();
        // Sized to the content rather than to the whole destination: the expansion is bounded by
        // the slice it is given, and that bound reaches the decompressor as a `c_int`.
        let Some(target) = dst.as_mut_slice().get_mut(..size_content) else {
            return Err(StorageError::from(crate::errors::Oversized {
                context: format!(
                    "content of {size_content} bytes exceeds the {capacity} byte destination buffer"
                ),
            }));
        };
        let expanded = match compress::decompress_into_slice(fragment, &payload, target) {
            Ok(expanded) => expanded,
            Err(err) => {
                lore_base::lore_debug!(
                    "Fragment {} failed decompression into caller buffer: {err:?}",
                    address.hash
                );
                return Ok(None);
            }
        };
        if options.verify && !content_verifies(expanded.size_content as usize, address, dst) {
            return Ok(None);
        }
        return Ok(Some((expanded, expanded.size_content as usize)));
    }

    if (fragment.flags & FragmentFlags::PayloadFragmented) != FragmentFlags::PayloadFragmented {
        // Sizes that disagree describe no payload that is the content, which the assembling path
        // reports against the fragment that claimed them.
        if !crate::payload_is_content(&fragment) {
            return Ok(None);
        }
        let written = copy_into_buffer(&payload, dst)?;
        if options.verify && !content_verifies(written, address, dst) {
            return Ok(None);
        }
        return Ok(Some((fragment, written)));
    }

    // Verified and expanded, the list is the root the walk starts from, so the walk never reads it
    // again.
    let (root, list) = match decompress_and_verify(fragment, payload, address, options) {
        Ok(result) => result,
        Err(err) => {
            lore_base::lore_debug!(
                "Fragment {} failed to open as a list: {err:?}",
                address.hash
            );
            return Ok(None);
        }
    };

    let written = deliver_root_into_buffer(
        store,
        partition,
        address,
        None,
        root,
        list,
        dst,
        options,
        depth,
        remote_session,
    )
    .await?;
    Ok(Some((root, written)))
}

/// Deliver `range` of the content rooted at `fragment` and `buffer` into `dst`, reporting the bytes
/// written.
///
/// Content spread across fragments is walked into `dst` in place, each leaf writing where it
/// belongs, so it is assembled nowhere else first. Content one fragment holds is already in
/// `buffer`, so the range is cut from it and copied in once.
#[allow(clippy::too_many_arguments)]
async fn deliver_root_into_buffer(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    range: Option<Range<usize>>,
    fragment: Fragment,
    buffer: Bytes,
    dst: &mut crate::CallerBuffer,
    options: ReadOptions,
    depth: usize,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<usize, StorageError> {
    let content = resolve_content_range(range, fragment.size_content);
    if content.is_empty() {
        return Ok(0);
    }

    if (fragment.flags & FragmentFlags::PayloadFragmented) != FragmentFlags::PayloadFragmented {
        let Some(bytes) = buffer.get(content.clone()) else {
            return Err(StorageError::internal(format!(
                "content range {content:?} reaches past the {} byte root",
                buffer.len()
            )));
        };
        return copy_into_buffer(bytes, dst);
    }

    let capacity = dst.len();
    let Some(assembled) = dst.as_mut_slice().get_mut(..content.len()) else {
        return Err(StorageError::from(crate::errors::Oversized {
            context: format!(
                "content of {} bytes exceeds the {capacity} byte destination buffer",
                content.len()
            ),
        }));
    };

    // SAFETY: `dst` is borrowed for this whole call, so the memory the walk divides among the
    // leaves stays valid and reaches nobody else, and the slice it is taken from bounds it.
    let target = unsafe { crate::CallerBuffer::new(assembled.as_mut_ptr(), assembled.len()) };
    read_defragment(
        store,
        partition,
        address,
        content.clone(),
        fragment,
        buffer,
        target,
        options,
        depth,
        remote_session,
    )
    .await?;

    Ok(content.len())
}

/// [`read`] delivering into a caller-owned buffer: the root is loaded once and the content taken
/// from it, so content spanning fragments is assembled nowhere but in `dst`.
#[allow(clippy::too_many_arguments)]
async fn read_range_into_buffer(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    range: Option<Range<usize>>,
    dst: &mut crate::CallerBuffer,
    options: ReadOptions,
    depth: usize,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<(Fragment, usize), StorageError> {
    let options = options.with_decompress();
    let (fragment, buffer) = load_fragment(
        store.clone(),
        partition,
        address,
        options,
        remote_session.clone(),
    )
    .await?;

    if let Some(max) = options.max_content_size
        && fragment.size_content > max
    {
        return Err(StorageError::from(crate::errors::Oversized {
            context: format!(
                "fragment size_content {} exceeds caller-supplied max {max}",
                fragment.size_content
            ),
        }));
    }

    let written = deliver_root_into_buffer(
        store,
        partition,
        address,
        range,
        fragment,
        buffer,
        dst,
        options,
        depth,
        remote_session,
    )
    .await?;
    Ok((fragment, written))
}

/// [`read`] delivering the content into a caller-owned buffer.
///
/// `dst.len()` states the capacity and bounds the read: content exceeding it fails with `Oversized`
/// rather than truncating. Returns the fragment describing the whole content and the number of
/// bytes written.
///
/// A whole read of content one local fragment holds goes straight into `dst`, allocating nothing
/// beyond the compressed payload where there is one; remote content arrives expanded on receipt
/// and is copied in once. Content spanning fragments is walked into `dst` leaf by leaf. A range of
/// content one fragment holds is cut from that fragment and copied in once. Every path verifies
/// against the address when `options.verify` is set.
///
/// The contents of `dst` are unspecified when this fails.
pub async fn read_into_buffer(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    range: Option<Range<usize>>,
    dst: &mut crate::CallerBuffer,
    options: ReadOptions,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<(Fragment, usize), StorageError> {
    // A range is cut from assembled content, so only a whole read is served by the payload as the
    // store holds it.
    if range.is_none()
        && let Some(read) = read_content_into_buffer(
            store.clone(),
            partition,
            address,
            dst,
            options,
            RootSource::LocalOrRemote,
            0,
            remote_session.clone(),
        )
        .await?
    {
        return Ok(read);
    }

    read_range_into_buffer(
        store,
        partition,
        address,
        range,
        dst,
        options,
        0,
        remote_session,
    )
    .await
}

/// Read content into a streaming channel, returning the fragment describing the whole content
/// and the content range that will arrive on the channel.
///
/// The returned range is the caller's, clamped to what exists, so a caller can emit a header
/// and account for what it receives before the first chunk lands. Chunks arrive in content
/// order and the caller positions them at `range.start` and upwards; the range is `0..0` when
/// nothing was asked for, and nothing is sent.
///
/// Ranged reads of a fragmented payload fetch only the leaves the range touches, so the work
/// is proportional to the range rather than to the content.
///
/// The range returns before the leaves flow, so a failure part-way through the tree arrives on
/// the channel as an `Err`: it is the only route by which the caller learns its content is short.
#[allow(clippy::too_many_arguments)]
pub async fn read_stream(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    range: Option<Range<usize>>,
    options: ReadOptions,
    sender: tokio::sync::mpsc::Sender<Result<Bytes, StorageError>>,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<(Fragment, Range<u64>), StorageError> {
    let options = options.with_decompress();
    let (fragment, buffer) = load_fragment(
        store.clone(),
        partition,
        address,
        options,
        remote_session.clone(),
    )
    .await?;

    let range = resolve_content_range(range, fragment.size_content);
    let streamed = range.start as u64..range.end as u64;
    if range.is_empty() {
        return Ok((fragment, streamed));
    }

    if (fragment.flags & FragmentFlags::PayloadFragmented) == FragmentFlags::PayloadFragmented {
        let store = store.clone();
        let pipeline_range = streamed.clone();
        let report = sender.clone();
        lore_base::lore_spawn!(async move {
            let result = defragment_pipeline(
                store,
                partition,
                address,
                fragment,
                buffer,
                pipeline_range,
                DefragmentSink::Stream { sender },
                options,
                remote_session,
            )
            .await;

            if let Err(err) = result {
                lore_base::lore_warn!("error while defragmenting during read_stream: {0}", err);
                let _ = report.send(Err(err)).await;
            }
        });

        Ok((fragment, streamed))
    } else {
        sender
            .send(Ok(buffer.slice(range)))
            .await
            .map_err(|_err| StorageError::internal("read stream closed"))?;
        Ok((fragment, streamed))
    }
}

/// Removes a temporary file that was never renamed into place.
///
/// An orphan is a *full-size* file holding a prefix — the target is sized before any content
/// arrives — and an invisible one, since the staging filters exclude the extension and nothing
/// else deletes them.
///
/// Armed before the open, because a failure part-way through it can leave the file created.
/// Disarmed after the rename because the path is derived from the destination: a guard outliving
/// its own rename would delete the next reader's file.
struct TemporaryFile {
    path: Option<PathBuf>,
}

impl TemporaryFile {
    fn guard(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn renamed(&mut self) {
        self.path = None;
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        if let Some(path) = self.path.take()
            && let Err(err) = std::fs::remove_file(&path)
            && err.kind() != std::io::ErrorKind::NotFound
        {
            lore_base::lore_warn!("failed to remove temporary file {}: {err}", path.display());
        }
    }
}

/// Read content into a file.
///
/// `range` selects the content to write; the file holds exactly that range and nothing else,
/// starting at its first byte. `None` writes the whole content, which is what sizing the file
/// to `size_content` used to mean.
///
/// Returns the fragment header along with the file's metadata when the write
/// path captures it on the open handle (single-fragment direct write). Callers
/// that need a stat regardless of path can fall back to a separate metadata
/// query when `None` is returned (the multi-fragment defragment path doesn't
/// surface metadata yet — the file handle moves through the pipeline).
#[allow(clippy::too_many_arguments)]
pub async fn read_into_file(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    path: &Path,
    temp_file_extension: &str,
    range: Option<Range<usize>>,
    options: ReadOptions,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<(Fragment, Option<std::fs::Metadata>), StorageError> {
    let _count_permit = file_count_limit_acquire()
        .await
        .forward::<StorageError>("permit failed")?;

    // Read the initial fragment
    let options = options.with_decompress();
    let (fragment, buffer) = load_fragment(
        store.clone(),
        partition,
        address,
        options,
        remote_session.clone(),
    )
    .await?;

    let metadata = write_root_to_file(
        store,
        partition,
        address,
        fragment,
        buffer,
        path,
        temp_file_extension,
        range,
        options,
        remote_session,
    )
    .await?;

    Ok((fragment, metadata))
}

/// Write the content a loaded root fragment stands for into `path`: the half of
/// [`read_into_file`] that follows the load, and the half [`read_resolved_into_file`] reaches
/// through a resolve rather than through an address.
///
/// A single fragment's payload is already in `buffer` and goes out in one whole-file write, whose
/// open handle answers the metadata the caller gets back. A fragment list streams through the
/// defragment pipeline into a file sized to the range up front, so each leaf lands at its own
/// offset and peak memory follows the leaf rather than the content — and no metadata comes back,
/// because the handle moves into the pipeline. That path stages through
/// `<path><temp_file_extension>` and renames, unless `options.direct_write` asks for the target to
/// be written in place.
///
/// A range starting past the end of the content selects nothing that exists, and `path` is left
/// alone: the file is not opened, so a destination that was already there survives a request the
/// caller got wrong. Nothing is written and no metadata comes back, and the caller decides what an
/// empty selection means from the fragment it holds. A start exactly at the end is a legitimate
/// empty read and does produce an empty file.
///
/// `options` must already carry `with_decompress`; both entry points load their root with it.
#[allow(clippy::too_many_arguments)]
async fn write_root_to_file(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    fragment: Fragment,
    buffer: Bytes,
    path: &Path,
    temp_file_extension: &str,
    range: Option<Range<usize>>,
    options: ReadOptions,
    remote_session: Option<Arc<StorageSession>>,
) -> Result<Option<std::fs::Metadata>, StorageError> {
    if range
        .as_ref()
        .is_some_and(|range| range.start as u64 > fragment.size_content)
    {
        return Ok(None);
    }

    let range = resolve_content_range(range, fragment.size_content);

    if fragment.flags & FragmentFlags::PayloadFragmented == FragmentFlags::PayloadFragmented {
        let mut retry = crate::retry(10, 10_000, 10);

        let file_path = if options.direct_write {
            path.to_path_buf()
        } else {
            let mut temporary_ext = path.extension().unwrap_or_default().to_os_string();
            temporary_ext.push(temp_file_extension);

            let mut temporary_path = path.to_path_buf();
            temporary_path.set_extension(temporary_ext);

            temporary_path
        };

        let mut temporary =
            (!options.direct_write).then(|| TemporaryFile::guard(file_path.clone()));

        let file = loop {
            match crate::defragment::open_file_write(file_path.as_path(), range.len()).await {
                Ok(file) => break file,
                Err(err) => {
                    if !retry.wait().await {
                        return Err(StorageError::internal_with_context(
                            err,
                            &format!("failed to open file: {}", path.display()),
                        ));
                    }
                }
            }
        };
        let defrag_target = DefragmentSink::File {
            file: file.clone(),
            size: range.len(),
        };

        lore_base::lore_trace!(
            "Opened file for immutable data write: {} size {}",
            path.display(),
            range.len()
        );

        defragment_pipeline(
            store,
            partition,
            address,
            fragment,
            buffer,
            range.start as u64..range.end as u64,
            defrag_target,
            options,
            remote_session,
        )
        .await?;

        if options.sync_data {
            file.sync_data()
                .await
                .map_err(|e| StorageError::internal_with_context(e, "flush file"))?;
        }
        // The handle holds no userspace buffer, so there is nothing to flush.
        drop(file);

        if !options.direct_write {
            let rename_err_msg = format!("rename {} -> {}", file_path.display(), path.display());
            lore_io::IoDriver::global()
                .rename(file_path.as_path(), path)
                .await
                .map_err(|e| StorageError::internal_with_context(e, &rename_err_msg))?;

            if let Some(temporary) = temporary.as_mut() {
                temporary.renamed();
            }
        }

        Ok(None)
    } else {
        let mut retry = crate::retry(10, 10_000, 10);
        let buffer = buffer.slice(range);
        let metadata = loop {
            match write_all_to_file(path, buffer.clone(), options.sync_data).await {
                Ok(meta) => break meta,
                Err(err) => {
                    if !retry.wait().await {
                        return Err(StorageError::internal_with_context(
                            err,
                            &format!("write to file: {}", path.display()),
                        ));
                    }
                }
            }
        };
        Ok(Some(metadata))
    }
}

/// Writes `buffer` as the whole contents of `path` and returns the resulting metadata.
///
/// One driver dispatch covers open, write, optional sync and stat, so the caller needs no
/// separate stat round-trip and the metadata comes off the open handle rather than from a second
/// path resolve. The whole-file operation refuses anything above `lore_io::WHOLE_FILE_LIMIT`,
/// which the content written here cannot reach: an unfragmented fragment's content is bounded by
/// `FRAGMENT_SIZE_THRESHOLD`.
pub async fn write_all_to_file(
    path: impl AsRef<Path>,
    buffer: Bytes,
    sync_data: bool,
) -> Result<std::fs::Metadata, std::io::Error> {
    let path = path.as_ref().to_path_buf();
    let buffer_len = buffer.len();

    // Reissued while the open fails transiently: a reader of this path grants no write access
    // for as long as it is open, so on Windows a write landing on a file being hashed or
    // fragmented waits for that scan rather than failing the materialization.
    let metadata = crate::fs_util::retry_transient(|| {
        let path = path.clone();
        let buffer = buffer.clone();
        async move {
            lore_io::IoDriver::global()
                .write_file_bytes(path, buffer, sync_data)
                .await
        }
    })
    .await?;

    lore_base::lore_trace!("Wrote {} bytes to {}", buffer_len, path.display());

    Ok(metadata)
}

/// The root a key resolved to, plus the session the tail should use for anything the root refers
/// to. Present whenever the caller supplied one, including on a local hit: the root can be cached
/// locally while a fragment list's leaves are not.
#[lore_macro::test_pub]
struct ResolvedRoot {
    resolved: Hash,
    address: Address,
    fragment: Fragment,
    buffer: Bytes,
    session: Option<Arc<StorageSession>>,
}

/// Local half of [`read_resolved`]: resolve `key` in the local mutable store and load the root it
/// names from the local store only.
///
/// `None` means the caller should ask the remote instead — the mapping is absent, it is a
/// tombstone, or its root is not cached locally.
///
/// A mapping that *is* present is trusted as-is and not revalidated. Because the key is mutable,
/// that is a weaker guarantee than the immutable [`load_fragment`] path gives: a cached mapping
/// can name a hash the key has since moved off. Freshness is the caller's choice through the same
/// flags a `get` uses — `remote` resolves authoritatively, the default prefers whatever is local.
///
/// On the fall-through it deliberately does not remote-read the locally cached hash. A remote
/// `get_resolved` answers the mapping and the root in one round trip, so re-resolving costs
/// nothing extra and answers against the authoritative mapping.
async fn load_resolved_local(
    store: Arc<dyn ImmutableStore>,
    mutable: Arc<dyn MutableStore>,
    partition: Partition,
    key: Hash,
    context: Context,
    options: ReadOptions,
) -> Option<(Hash, Fragment, Bytes)> {
    let resolved = match mutable.load(partition, key, KeyType::Resolve).await {
        Ok(resolved) if !resolved.is_zero() => resolved,
        Ok(_) => return None,
        Err(err) => {
            lore_base::lore_trace!("Key {key} failed to resolve from local mutable store: {err:?}");
            return None;
        }
    };

    let address = Address {
        hash: resolved,
        context,
    };
    match load_fragment(store, partition, address, options.no_remote(), None).await {
        Ok((fragment, buffer)) => Some((resolved, fragment, buffer)),
        Err(err) => {
            lore_base::lore_trace!(
                "Key {key} resolved locally to {resolved}, whose root is not cached: {err:?}"
            );
            None
        }
    }
}

/// Resolve `key` to the root fragment it names, sharing one round trip with the read of that root
/// whenever the answer is not already local.
///
/// The key is always read as [`KeyType::Resolve`], locally and remotely alike.
///
/// Local-first like [`read`]: [`load_resolved_local`] tries the local mutable store and the local
/// copy of the root it names, and only a miss there reaches the remote. A fragment list's leaves
/// go through [`load_fragment`] either way, so they keep their own local-then-remote fallback and
/// local caching.
///
/// On a remote resolve the key->hash mapping is written back to the local mutable store once the
/// payload write-back succeeds, under the same gate — so a later call can be served entirely
/// locally, and the mapping is never left pointing at a root this store does not hold.
///
/// A local root travels with the caller's session anyway: a fragment list's leaves may still
/// exist only remotely.
///
/// Verifies every remote root, whatever `options.verify` says. A verification failure gets one
/// heal attempt then a re-resolve, as [`load_fragment`] does. The retry re-resolves rather than
/// re-reads, since the heal targets the resolved address and a fresh resolve costs the same
/// single round trip.
///
/// The root is expanded when `options.decompress` is set, and left as stored otherwise.
///
/// `flags` is a reserved bitmask forwarded to the server; 0 for default behaviour.
#[lore_macro::test_pub]
#[allow(clippy::too_many_arguments)]
async fn resolve_root(
    store: Arc<dyn ImmutableStore>,
    mutable: Arc<dyn MutableStore>,
    partition: Partition,
    key: Hash,
    context: Context,
    flags: u32,
    options: ReadOptions,
    session: Option<Arc<StorageSession>>,
) -> Result<ResolvedRoot, StorageError> {
    let key_address = Address { hash: key, context };

    if options.local
        && let Some((resolved, fragment, buffer)) = load_resolved_local(
            store.clone(),
            mutable.clone(),
            partition,
            key,
            context,
            options,
        )
        .await
    {
        return Ok(ResolvedRoot {
            resolved,
            address: Address {
                hash: resolved,
                context,
            },
            fragment,
            buffer,
            session,
        });
    }

    if !options.remote {
        return Err(StorageError::from(crate::errors::AddressNotFound::from(
            key_address,
        )));
    }
    let Some(session) = session else {
        return Err(StorageError::from(crate::errors::AddressNotFound::from(
            key_address,
        )));
    };

    lore_base::lore_trace!("Resolve key {} from remote", key_address);

    let mut heal_attempted = false;
    let (resolved, address, fragment, buffer) = loop {
        let (resolved, mut fragment, buffer) =
            remote_get_resolved_retry(session.as_ref(), key, context, flags).await?;
        REMOTE_FETCH_BYTES.fetch_add(buffer.len() as u64, Ordering::Relaxed);

        if resolved.is_zero() {
            return Err(StorageError::from(crate::errors::AddressNotFound::from(
                key_address,
            )));
        }

        let address = Address {
            hash: resolved,
            context,
        };

        fragment.flags |= FragmentFlags::PayloadStoredDurable;

        match verify_receipt(fragment, buffer.clone(), address) {
            Ok(content) => {
                let should_store = options.cache
                    || (fragment.flags & FragmentFlags::PayloadLocalCachePriority) != 0;
                if should_store
                    && store
                        .clone()
                        .put(partition, address, fragment, Some(buffer.clone()), false)
                        .await
                        .is_ok()
                {
                    let _ = mutable
                        .store(partition, key, resolved, KeyType::Resolve)
                        .await;
                }
                let (fragment, buffer) = Fetched {
                    content,
                    raw: (fragment, buffer),
                }
                .take(options.decompress);
                break (resolved, address, fragment, buffer);
            }
            Err(err) => {
                if matches!(err, StorageError::NotSupported(_)) {
                    return Err(err);
                }
                if heal_attempted {
                    lore_base::lore_error!(
                        "Key {key} resolved to {resolved}, still corrupt after heal: {err}"
                    );
                    return Err(err);
                }

                lore_base::lore_warn!("Key {key} resolved to {resolved}: {err}. Attempting heal.");
                let healed = session
                    .verify(&address, true)
                    .await
                    .is_ok_and(|r| r.healed == lore_base::types::HealResult::Healed);
                if !healed {
                    lore_base::lore_error!("Server did not heal fragment {resolved}");
                    return Err(err);
                }

                lore_base::lore_debug!("Server healed fragment {resolved}, resolving again");
                heal_attempted = true;
            }
        }
    };

    Ok(ResolvedRoot {
        resolved,
        address,
        fragment,
        buffer,
        session: Some(session),
    })
}

/// `mutable_load(key)` + [`read`] of the resulting address, resolved in one round trip when the
/// remote answers. Returns the resolved hash alongside the content.
///
/// See [`resolve_root`] for how the key is resolved; this reassembles the whole content into one
/// buffer. [`read_resolved_stream`] delivers it fragment by fragment instead.
#[allow(clippy::too_many_arguments)]
pub async fn read_resolved(
    store: Arc<dyn ImmutableStore>,
    mutable: Arc<dyn MutableStore>,
    partition: Partition,
    key: Hash,
    context: Context,
    flags: u32,
    range: Option<Range<usize>>,
    options: ReadOptions,
    session: Option<Arc<StorageSession>>,
) -> Result<(Hash, Bytes), StorageError> {
    let root = resolve_root(
        store.clone(),
        mutable,
        partition,
        key,
        context,
        flags,
        options.with_decompress(),
        session,
    )
    .await?;

    let bytes = read_resolved_content(
        store,
        partition,
        root.address,
        root.fragment,
        root.buffer,
        range,
        options.with_decompress(),
        root.session,
    )
    .await?;
    Ok((root.resolved, bytes))
}

/// [`read_resolved`] delivering the content through `sender` one leaf at a time instead of
/// reassembling it, mirroring what [`read_stream`] does for an address. Each leaf arrives with its
/// fragment, expanded when `options.decompress` is set and as stored otherwise.
///
/// Returns the resolved hash and the content's total size; the leaves follow on the channel. Peak
/// memory is bounded by the channel depth rather than by the content, which is what makes this
/// usable for a key naming something large.
#[allow(clippy::too_many_arguments)]
pub async fn read_resolved_stream(
    store: Arc<dyn ImmutableStore>,
    mutable: Arc<dyn MutableStore>,
    partition: Partition,
    key: Hash,
    context: Context,
    flags: u32,
    options: ReadOptions,
    sender: tokio::sync::mpsc::Sender<Result<(Fragment, Bytes), StorageError>>,
    session: Option<Arc<StorageSession>>,
) -> Result<(Hash, u64), StorageError> {
    let root = resolve_root(
        store.clone(),
        mutable,
        partition,
        key,
        context,
        flags,
        options,
        session,
    )
    .await?;

    if let Some(max) = options.max_content_size
        && root.fragment.size_content > max
    {
        return Err(StorageError::from(crate::errors::Oversized {
            context: format!(
                "fragment size_content {} exceeds caller-supplied max {max}",
                root.fragment.size_content
            ),
        }));
    }

    if (root.fragment.flags & FragmentFlags::PayloadFragmented) == FragmentFlags::PayloadFragmented
    {
        let address = root.address;
        let fragment = root.fragment;
        let buffer = root.buffer;
        let remote_session = root.session;
        let report = sender.clone();
        lore_base::lore_spawn!(async move {
            let result = defragment_pipeline_leaves(
                store,
                partition,
                address,
                fragment,
                buffer,
                sender,
                options,
                remote_session,
            )
            .await;

            if let Err(err) = result {
                lore_base::lore_warn!(
                    "error while defragmenting during read_resolved_stream: {0}",
                    err
                );
                let _ = report.send(Err(err)).await;
            }
        });
    } else {
        sender
            .send(Ok((root.fragment, root.buffer)))
            .await
            .map_err(|_err| StorageError::internal("read stream closed"))?;
    }

    Ok((root.resolved, root.fragment.size_content))
}

/// [`read_into_buffer`] reached by key rather than by address.
///
/// `dst.len()` states the capacity and bounds the read: content exceeding it fails with `Oversized`
/// rather than truncating. Returns the resolved hash and the number of bytes written.
///
/// A key resolving locally to content one fragment holds reads straight into `dst`. Anything else
/// falls back to [`read_resolved`], which assembles no more than `dst` can hold.
///
/// The contents of `dst` are unspecified when this fails.
#[allow(clippy::too_many_arguments)]
pub async fn read_resolved_into_buffer(
    store: Arc<dyn ImmutableStore>,
    mutable: Arc<dyn MutableStore>,
    partition: Partition,
    key: Hash,
    context: Context,
    flags: u32,
    dst: &mut crate::CallerBuffer,
    options: ReadOptions,
    session: Option<Arc<StorageSession>>,
) -> Result<(Hash, usize), StorageError> {
    // [`read_resolved`] resolves the key again, so resolve here only while the direct read is open.
    // The root has to be local: the mapping just read is trusted as it stands, and reading the hash
    // it names from the remote would answer with content the key may have moved off. A local root's
    // leaves may still be remote, as [`resolve_root`] allows.
    if options.local
        && let Ok(resolved) = mutable.clone().load(partition, key, KeyType::Resolve).await
        && !resolved.is_zero()
    {
        let address = Address {
            hash: resolved,
            context,
        };
        if let Some((_fragment, written)) = read_content_into_buffer(
            store.clone(),
            partition,
            address,
            dst,
            options,
            RootSource::LocalOnly,
            0,
            session.clone(),
        )
        .await?
        {
            return Ok((resolved, written));
        }
    }

    // The resolve here is authoritative, so the address it answers with is read the way any other
    // named address is.
    let root = resolve_root(
        store.clone(),
        mutable,
        partition,
        key,
        context,
        flags,
        options.with_decompress(),
        session,
    )
    .await?;

    let options = options.with_decompress();
    if let Some(max) = options.max_content_size
        && root.fragment.size_content > max
    {
        return Err(StorageError::from(crate::errors::Oversized {
            context: format!(
                "fragment size_content {} exceeds caller-supplied max {max}",
                root.fragment.size_content
            ),
        }));
    }

    let written = deliver_root_into_buffer(
        store,
        partition,
        root.address,
        None,
        root.fragment,
        root.buffer,
        dst,
        options,
        0,
        root.session,
    )
    .await?;
    Ok((root.resolved, written))
}

/// [`read_resolved`] writing the content into a file instead of reassembling it in memory —
/// [`read_into_file`] reached by key rather than by address.
///
/// Returns the resolved hash and the fragment describing the whole content, so a caller can
/// report the address the key names and check its range against what exists without stating the
/// file it just wrote.
///
/// The round trip is the one [`resolve_root`] pays: the key and the root fragment it names come
/// back together, so nothing is spent resolving before the read starts. From there the content
/// goes to disk the way [`read_into_file`] puts it there — a single fragment in one write, a
/// fragment list leaf by leaf at its own offset through the defragment pipeline — so neither the
/// caller nor this library ever holds the whole content, and the file is the only place it is
/// assembled.
///
/// `range` selects the content to write and the file holds exactly that range from its first
/// byte, as [`read_into_file`]'s does; only the leaves the range covers are fetched.
///
/// A key with no mapping, or one naming content that cannot be read, fails without touching
/// `path` — there is no zero-hash truncation here, because a resolve that finds nothing is a miss
/// rather than an address for empty content.
#[allow(clippy::too_many_arguments)]
pub async fn read_resolved_into_file(
    store: Arc<dyn ImmutableStore>,
    mutable: Arc<dyn MutableStore>,
    partition: Partition,
    key: Hash,
    context: Context,
    flags: u32,
    path: &Path,
    temp_file_extension: &str,
    range: Option<Range<usize>>,
    options: ReadOptions,
    session: Option<Arc<StorageSession>>,
) -> Result<(Hash, Fragment), StorageError> {
    let _count_permit = file_count_limit_acquire()
        .await
        .forward::<StorageError>("permit failed")?;

    let options = options.with_decompress();
    let root = resolve_root(
        store.clone(),
        mutable,
        partition,
        key,
        context,
        flags,
        options,
        session,
    )
    .await?;

    write_root_to_file(
        store,
        partition,
        root.address,
        root.fragment,
        root.buffer,
        path,
        temp_file_extension,
        range,
        options,
        root.session,
    )
    .await?;

    Ok((root.resolved, root.fragment))
}

/// [`remote_get_retry`] for `get_resolved`: back off on `SlowDown`, recover from a stale
/// session id by invalidating and retrying. `key` supplies error context only.
async fn remote_get_resolved_retry(
    session: &StorageSession,
    key: Hash,
    context: Context,
    flags: u32,
) -> Result<(Hash, Fragment, Bytes), StorageError> {
    let _guard = RemoteFetchGuard::new();
    #[cfg(feature = "test-util")]
    if let Some(served) = crate::test_util::remote_resolved(key) {
        return Ok(served);
    }
    let mut retry = crate::store_retry();
    let mut stale_session_retries: u32 = 0;
    let key_address = Address { hash: key, context };
    loop {
        debug_assert!(!key.is_zero(), "Cannot resolve zero key from store");
        match session.get_resolved(&key, &context, flags).await {
            Ok(resolved) => return Ok(resolved),
            Err(ref e) if e.is_slow_down() => {
                if !retry.wait().await {
                    return Err(StorageError::from(SlowDown));
                }
            }
            Err(err) => {
                let storage_err = crate::error::protocol_error_to_storage(err, key_address);
                if matches!(storage_err, StorageError::NotConnected(_))
                    && stale_session_retries < MAX_STALE_SESSION_RETRIES
                {
                    stale_session_retries += 1;
                    session.invalidate().await;
                    if !retry.wait().await {
                        return Err(storage_err);
                    }
                    continue;
                }
                return Err(storage_err);
            }
        }
    }
}

/// Shared tail of [`read_resolved`]: enforce `max_content_size`, clamp `range` to the content, and
/// reassemble a fragment list's leaves through [`load_fragment`], which may fetch them remotely.
#[allow(clippy::too_many_arguments)]
async fn read_resolved_content(
    store: Arc<dyn ImmutableStore>,
    partition: Partition,
    address: Address,
    fragment: Fragment,
    buffer: Bytes,
    range: Option<Range<usize>>,
    options: ReadOptions,
    session: Option<Arc<StorageSession>>,
) -> Result<Bytes, StorageError> {
    if let Some(max) = options.max_content_size
        && fragment.size_content > max
    {
        return Err(StorageError::from(crate::errors::Oversized {
            context: format!(
                "fragment size_content {} exceeds caller-supplied max {max}",
                fragment.size_content
            ),
        }));
    }

    let range = match range {
        Some(range) => {
            min(range.start, fragment.size_content as usize)
                ..min(range.end, fragment.size_content as usize)
        }
        None => 0..fragment.size_content as usize,
    };
    if range.is_empty() {
        return Ok(Bytes::default());
    }

    if (fragment.flags & FragmentFlags::PayloadFragmented) == FragmentFlags::PayloadFragmented {
        let mut target_buffer = BytesMut::with_capacity(range.len());
        // Safety: the capacity was just reserved, and read_defragment fully writes the range
        // before the buffer is read back.
        unsafe {
            target_buffer.set_len(range.len());
        }
        let target_size = target_buffer.len();
        let target = target_buffer.split();
        read_defragment(
            store, partition, address, range, fragment, buffer, target, options, 0, session,
        )
        .await?;
        if !target_buffer.try_reclaim(target_size) {
            return Err(StorageError::internal(
                "failed to reclaim buffer after defragmenting",
            ));
        }
        // Safety: try_reclaim just confirmed the split-off target bytes are back in this
        // buffer's capacity, and read_defragment initialized all of them.
        unsafe {
            target_buffer.set_len(target_size);
        }
        Ok(target_buffer.freeze())
    } else {
        Ok(buffer.slice(range))
    }
}
