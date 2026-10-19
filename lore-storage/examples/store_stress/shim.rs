// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The fault shim, `scripts/store-stress/crashpoint.c`, when the process runs under it: telling it
//! which store operations run, so that it can place and target its kills and faults by them, and
//! keep them where a kill from outside can be placed from; ending its faults, so that the run can
//! check that the store recovers from them; and asking which file operations are in flight.

use std::ffi::CString;
use std::ffi::c_char;
use std::ffi::c_long;
use std::ffi::c_void;
use std::sync::atomic::AtomicUsize;

/// The shim's function `name`, when the process runs under it.
fn function(name: &std::ffi::CStr) -> Option<*mut c_void> {
    // SAFETY: dlsym with a valid, nul-terminated name; a null result means no such symbol.
    let address = unsafe { libc::dlsym(libc::RTLD_DEFAULT, name.as_ptr()) };
    (!address.is_null()).then_some(address)
}

/// Whether the process runs under the shim.
pub fn active() -> bool {
    function(c"crashpoint_register").is_some()
}

/// Whether the shim fails operations on the files of `store`, `immutable` or `mutable`, as its
/// environment asks.
pub fn faults_on(store: &str) -> bool {
    let set = |name: &str| std::env::var(name).is_ok_and(|value| !value.is_empty());
    active()
        && std::env::var("STRESS_PATH").is_ok_and(|paths| {
            paths
                .split(',')
                .any(|path| path.ends_with(&format!("/{store}")))
        })
        && (set("STRESS_FAIL_AT") || set("STRESS_FAIL_PROB"))
}

/// Counters for `count` kinds of store operation, kept by the shim where a kill from outside the
/// process leaves them readable, when the process runs under it.
pub fn counters(count: usize) -> Option<&'static [AtomicUsize]> {
    let address = function(c"crashpoint_counters")?;
    // SAFETY: the shim defines `crashpoint_counters` with this signature, and returns null or
    // `count` zeroed counters that live as long as the process and that nothing else writes.
    unsafe {
        let counters =
            std::mem::transmute::<*mut c_void, extern "C" fn(usize) -> *mut AtomicUsize>(address);
        let first = counters(count);
        (!first.is_null()).then(|| std::slice::from_raw_parts(first.cast_const(), count))
    }
}

/// Tell the shim that `running[i]` counts the running store operations named by the i-th of
/// `names`. Both must outlive every file operation the process makes.
pub fn register(running: &'static [AtomicUsize], names: &[&str]) {
    let Some(address) = function(c"crashpoint_register") else {
        return;
    };
    let Ok(names) = CString::new(names.join(",")) else {
        return;
    };
    let names: &'static CString = Box::leak(Box::new(names));
    // SAFETY: the shim defines `crashpoint_register` with this signature, and reads the counts
    // and the names only while the process lives, which both outlive.
    unsafe {
        let register =
            std::mem::transmute::<*mut c_void, extern "C" fn(*const usize, *const c_char)>(address);
        register(running.as_ptr().cast::<usize>(), names.as_ptr());
    }
}

/// End the shim's faults and kills, returning how many operations it failed, when the process
/// runs under it.
pub fn disarm() -> Option<c_long> {
    let address = function(c"crashpoint_disarm")?;
    // SAFETY: the shim defines `crashpoint_disarm` with this signature.
    let faults = unsafe {
        let disarm = std::mem::transmute::<*mut c_void, extern "C" fn() -> c_long>(address);
        disarm()
    };
    Some(faults)
}

/// The file operations in flight in the process, each `op:kind` as the shim names them, when the
/// process runs under it.
pub fn in_flight() -> Option<Vec<String>> {
    let address = function(c"crashpoint_in_flight")?;
    let mut buffer = vec![0u8; 4096];
    // SAFETY: the shim defines `crashpoint_in_flight` with this signature, and writes at most
    // `buffer.len()` bytes, nul-terminated, returning the length before the nul.
    let length = unsafe {
        let in_flight =
            std::mem::transmute::<*mut c_void, extern "C" fn(*mut c_char, usize) -> i32>(address);
        in_flight(buffer.as_mut_ptr().cast::<c_char>(), buffer.len())
    };
    buffer.truncate(usize::try_from(length).unwrap_or(0));
    Some(
        String::from_utf8_lossy(&buffer)
            .split(',')
            .filter(|op| !op.is_empty())
            .map(str::to_string)
            .collect(),
    )
}
