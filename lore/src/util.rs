// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::any::type_name;
use std::collections::HashMap;
use std::path::Path;
use std::time::Instant;

use lore_base::version::LORE_LIBRARY_VERSION;
use lore_revision::interface::LoreArray;
use lore_revision::lore_debug;
use lore_revision::lore_warn;
use lore_revision::util::path::PathError;
use lore_revision::util::path::RelativePath;

use crate::interface::LoreString;

#[allow(dead_code)]
pub fn convert_user_optional_path(
    repository_path: impl AsRef<Path>,
    path: &str,
) -> Result<RelativePath, PathError> {
    RelativePath::new_from_user_path(repository_path.as_ref(), path)
}

pub fn convert_user_paths(
    repository_path: impl AsRef<Path>,
    paths: LoreArray<LoreString>,
) -> Result<Vec<RelativePath>, PathError> {
    let mut relative_paths = Vec::with_capacity(paths.len());

    for path in paths.as_slice() {
        relative_paths.push(RelativePath::new_from_user_path(
            repository_path.as_ref(),
            path.as_str(),
        )?);
    }

    Ok(relative_paths)
}

pub fn path_messages(
    kind: &str,
    paths: &LoreArray<LoreString>,
    messages: &LoreArray<LoreString>,
) -> Result<HashMap<String, String>, PathError> {
    let (paths, messages) = (paths.as_slice(), messages.as_slice());
    if paths.len() != messages.len() {
        lore_warn!(
            "{kind}_paths length ({}) does not match {kind}_messages length ({}), ignoring per-{kind} messages",
            paths.len(),
            messages.len()
        );
    }
    paths
        .iter()
        .zip(messages)
        .map(|(path, message)| {
            let path = RelativePath::new_from_initial_path(path.as_str())?;
            Ok((path.as_str().to_owned(), message.to_string()))
        })
        .collect()
}

pub fn log_command_info<T, A>(_caller: &T, args: &A)
where
    A: std::fmt::Debug,
{
    lore_debug!(
        "Executing command: {} {}",
        type_name::<T>(),
        LORE_LIBRARY_VERSION.as_str()
    );
    lore_debug!("Command arguments: {args:?}");
}

pub fn log_command_done<T>(_caller: &T, start: Instant) {
    lore_debug!(
        "Finished command: {} ({:.2}s)",
        type_name::<T>(),
        start.elapsed().as_secs_f32()
    );
}
