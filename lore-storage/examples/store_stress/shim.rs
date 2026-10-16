// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The fault shim, `scripts/store-stress/crashpoint.c`, when the process runs under it: telling it
//! which store operations run, so that it can place and target its kills and faults by them, and
//! ending its faults, so that the run can check that the store recovers from them.

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
