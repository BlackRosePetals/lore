// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Directory snapshots. A copy of a store directory taken while the store is held at a point, with
//! no write of it in flight, holds what a process killed at that point leaves on disk: the page
//! cache outlives the process, so every write that returned is in the files.

use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::Path;

/// Copy the tree at `source` to `destination`, which must not exist, keeping the holes of sparse
/// files, as a packfile created near the size limit is.
pub fn copy_tree(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        let kind = entry.file_type()?;
        if kind.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else if kind.is_file() {
            copy_sparse(&entry.path(), &target)?;
        }
    }
    Ok(())
}

fn copy_sparse(source: &Path, destination: &Path) -> std::io::Result<()> {
    let mut input = std::fs::File::open(source)?;
    let mut output = std::fs::File::create(destination)?;
    let length = input.metadata()?.len();
    output.set_len(length)?;
    let mut offset: u64 = 0;
    let mut buffer = vec![0u8; 1 << 20];
    while offset < length {
        // SAFETY: lseek on a file descriptor this function owns.
        let data =
            unsafe { libc::lseek(input.as_raw_fd(), offset as libc::off_t, libc::SEEK_DATA) };
        if data < 0 {
            break;
        }
        // SAFETY: as above.
        let hole = unsafe { libc::lseek(input.as_raw_fd(), data, libc::SEEK_HOLE) };
        let (start, end) = (data as u64, if hole < 0 { length } else { hole as u64 });
        input.seek(SeekFrom::Start(start))?;
        output.seek(SeekFrom::Start(start))?;
        let mut remaining = end - start;
        while remaining > 0 {
            let chunk = remaining.min(buffer.len() as u64) as usize;
            input.read_exact(&mut buffer[..chunk])?;
            output.write_all(&buffer[..chunk])?;
            remaining -= chunk as u64;
        }
        offset = end;
    }
    Ok(())
}
