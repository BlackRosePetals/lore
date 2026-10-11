// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use lore_transport::quic::client::*;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;

fn inflight_counters() -> [AtomicU64; STREAM_COUNT as usize] {
    std::array::from_fn(|_| AtomicU64::new(0))
}

#[test]
fn inflight_guard_counts_a_request_only_while_it_is_outstanding() {
    let inflight = AtomicU64::new(0);

    {
        let _first = StreamInflightGuard::new(&inflight);
        assert_eq!(inflight.load(Ordering::Relaxed), 1);

        let _second = StreamInflightGuard::new(&inflight);
        assert_eq!(inflight.load(Ordering::Relaxed), 2);
    }

    assert_eq!(inflight.load(Ordering::Relaxed), 0);
}

#[test]
fn high_priority_spreads_concurrent_requests_over_every_stream() {
    let inflight = inflight_counters();

    let mut guards = Vec::new();
    let mut selected = Vec::new();
    for _ in 0..STREAM_COUNT {
        let stream = select_stream(&inflight, STREAM_COUNT, true);
        guards.push(StreamInflightGuard::new(&inflight[stream as usize]));
        selected.push(stream);
    }

    selected.sort_unstable();
    assert_eq!(selected, (0..STREAM_COUNT).collect::<Vec<_>>());
}

#[test]
fn high_priority_reuses_a_stream_once_its_request_completed() {
    let inflight = inflight_counters();

    for _ in 0..STREAM_COUNT * 4 {
        let stream = select_stream(&inflight, STREAM_COUNT, true);
        let _guard = StreamInflightGuard::new(&inflight[stream as usize]);
        assert_eq!(stream, 0);
    }

    assert!(
        inflight
            .iter()
            .all(|count| count.load(Ordering::Relaxed) == 0)
    );
}

#[test]
fn normal_priority_spreads_concurrent_requests_over_the_non_priority_streams() {
    let inflight = inflight_counters();

    let mut guards = Vec::new();
    let mut selected = Vec::new();
    for _ in PRIORITY_STREAM_COUNT..STREAM_COUNT {
        let stream = select_stream(&inflight, STREAM_COUNT, false);
        guards.push(StreamInflightGuard::new(&inflight[stream as usize]));
        selected.push(stream);
    }

    selected.sort_unstable();
    assert_eq!(
        selected,
        (PRIORITY_STREAM_COUNT..STREAM_COUNT).collect::<Vec<_>>()
    );
}

#[test]
fn normal_priority_leaves_the_priority_streams_to_metadata() {
    let inflight = inflight_counters();

    // Every non-priority stream is busy while the priority streams sit idle. Balancing on
    // outstanding requests alone would send bulk traffic to a priority stream; the reserved
    // window is what stops it.
    let _guards: Vec<_> = (PRIORITY_STREAM_COUNT..STREAM_COUNT)
        .map(|stream| StreamInflightGuard::new(&inflight[stream as usize]))
        .collect();

    let stream = select_stream(&inflight, STREAM_COUNT, false);
    assert!(
        stream >= PRIORITY_STREAM_COUNT,
        "bulk traffic must stay off the reserved streams, got {stream}"
    );
}

#[test]
fn normal_priority_shares_what_is_open_before_any_stream_can_be_reserved() {
    let inflight = inflight_counters();

    // With fewer streams open than the reservation needs, there is nothing to reserve.
    assert_eq!(select_stream(&inflight, 1, false), 0);
    assert_eq!(select_stream(&inflight, PRIORITY_STREAM_COUNT, false), 0);
}

#[test]
fn normal_priority_reuses_a_stream_once_its_request_completed() {
    let inflight = inflight_counters();

    for _ in 0..STREAM_COUNT * 4 {
        let stream = select_stream(&inflight, STREAM_COUNT, false);
        let _guard = StreamInflightGuard::new(&inflight[stream as usize]);
        assert_eq!(stream, PRIORITY_STREAM_COUNT);
    }

    assert!(
        inflight
            .iter()
            .all(|count| count.load(Ordering::Relaxed) == 0)
    );
}
fn ipv6_addr() -> SocketAddr {
    "[::1]:41337".parse().unwrap()
}

fn ipv4_addr() -> SocketAddr {
    "127.0.0.1:41337".parse().unwrap()
}

#[test]
fn happy_eyeballs_interleaves_ipv6_first_addresses() {
    let ipv6_second = "[::2]:41337".parse().unwrap();
    let ipv6_third = "[::3]:41337".parse().unwrap();
    let ipv4_second = "127.0.0.2:41337".parse().unwrap();

    assert_eq!(
        interleave_socket_addrs(vec![
            ipv6_addr(),
            ipv6_second,
            ipv6_third,
            ipv4_addr(),
            ipv4_second,
        ]),
        vec![
            ipv6_addr(),
            ipv4_addr(),
            ipv6_second,
            ipv4_second,
            ipv6_third,
        ]
    );
}

#[test]
fn happy_eyeballs_interleaves_ipv4_first_addresses() {
    let ipv4_second = "127.0.0.2:41337".parse().unwrap();
    let ipv6_second = "[::2]:41337".parse().unwrap();

    assert_eq!(
        interleave_socket_addrs(vec![ipv4_addr(), ipv4_second, ipv6_addr(), ipv6_second,]),
        vec![ipv4_addr(), ipv6_addr(), ipv4_second, ipv6_second]
    );
}

#[tokio::test]
async fn happy_eyeballs_starts_fallback_while_first_attempt_is_stalled() {
    let attempts = Arc::new(Mutex::new(Vec::new()));
    let attempt_log = attempts.clone();

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        connect_happy_eyeballs(
            vec![ipv6_addr(), ipv4_addr()],
            Duration::from_millis(10),
            move |addr| {
                let attempt_log = attempt_log.clone();
                async move {
                    attempt_log.lock().push(addr);
                    if addr.is_ipv6() {
                        std::future::pending().await
                    } else {
                        Some(addr)
                    }
                }
            },
        ),
    )
    .await
    .expect("fallback should not wait for the stalled first attempt");

    assert_eq!(result, Some(ipv4_addr()));
    assert_eq!(*attempts.lock(), vec![ipv6_addr(), ipv4_addr()]);
}

#[tokio::test]
async fn happy_eyeballs_advances_immediately_after_failure() {
    let started = std::time::Instant::now();

    let result = connect_happy_eyeballs(
        vec![ipv6_addr(), ipv4_addr()],
        Duration::from_secs(1),
        |addr| async move { if addr.is_ipv6() { None } else { Some(addr) } },
    )
    .await;

    assert_eq!(result, Some(ipv4_addr()));
    assert!(started.elapsed() < Duration::from_millis(750));
}

#[tokio::test]
async fn happy_eyeballs_does_not_start_fallback_after_first_success() {
    let attempts = Arc::new(Mutex::new(HashMap::new()));
    let attempt_counts = attempts.clone();

    let result = connect_happy_eyeballs(
        vec![ipv6_addr(), ipv4_addr()],
        Duration::from_millis(10),
        move |addr| {
            let attempt_counts = attempt_counts.clone();
            async move {
                *attempt_counts.lock().entry(addr).or_insert(0) += 1;
                Some(addr)
            }
        },
    )
    .await;

    assert_eq!(result, Some(ipv6_addr()));
    assert_eq!(attempts.lock().get(&ipv6_addr()), Some(&1));
    assert_eq!(attempts.lock().get(&ipv4_addr()), None);
}

#[tokio::test]
async fn happy_eyeballs_returns_none_when_all_attempts_fail() {
    let result = connect_happy_eyeballs(
        vec![ipv6_addr(), ipv4_addr()],
        Duration::from_millis(10),
        |_| async { None::<SocketAddr> },
    )
    .await;

    assert_eq!(result, None);
}

#[tokio::test]
async fn happy_eyeballs_bounds_in_flight_attempts() {
    let remote_addrs: Vec<_> = (1..=HAPPY_EYEBALLS_MAX_IN_FLIGHT + 1)
        .map(|port| SocketAddr::new(ipv6_addr().ip(), port as u16))
        .collect();
    let release = Arc::new(Semaphore::new(0));
    let attempt_release = release.clone();
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();

    let task = lore_base::lore_spawn!(connect_happy_eyeballs(
        remote_addrs.clone(),
        Duration::from_millis(1),
        move |addr| {
            started_tx.send(addr).unwrap();
            let attempt_release = attempt_release.clone();
            async move {
                attempt_release.acquire().await.unwrap().forget();
                None::<SocketAddr>
            }
        },
    ));

    for expected in remote_addrs.iter().take(HAPPY_EYEBALLS_MAX_IN_FLIGHT) {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
                .await
                .expect("attempt should start"),
            Some(*expected)
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), started_rx.recv())
            .await
            .is_err(),
        "attempts above the in-flight limit should remain queued"
    );

    release.add_permits(1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), started_rx.recv())
            .await
            .expect("queued attempt should start when a slot opens"),
        Some(remote_addrs[HAPPY_EYEBALLS_MAX_IN_FLIGHT])
    );

    task.abort();
}

fn receive_buffer_size(socket: &std::net::UdpSocket) -> usize {
    socket2::SockRef::from(socket)
        .recv_buffer_size()
        .expect("socket receive buffer size should be readable")
}

fn bound_socket_with_receive_buffer(bytes: usize) -> std::net::UdpSocket {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind should succeed");
    socket2::SockRef::from(&socket)
        .set_recv_buffer_size(bytes)
        .expect("receive buffer size should be settable");
    socket
}

#[test]
fn raise_receive_buffer_grows_a_smaller_buffer() {
    let socket = bound_socket_with_receive_buffer(64 * 1024);
    let before = receive_buffer_size(&socket);

    raise_receive_buffer(&socket, RECEIVE_BUFFER_BYTES);

    assert!(receive_buffer_size(&socket) > before);
}

#[test]
fn raise_receive_buffer_keeps_a_larger_buffer() {
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind should succeed");
    let before = receive_buffer_size(&socket);

    // Asking for less than the buffer holds would shrink it, so an unchanged size shows the
    // request was not made.
    raise_receive_buffer(&socket, before / 4);

    assert_eq!(receive_buffer_size(&socket), before);
}
