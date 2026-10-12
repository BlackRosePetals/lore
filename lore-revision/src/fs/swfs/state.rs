use std::collections::BTreeSet;
use std::sync::Arc;

use dashmap::DashSet;
use lore_error_set::ForwardStrict;
use lore_error_set::WrapInternal;

use crate::fs::filesystem_provider::FileInfo;
use crate::fs::swfs::api_interface::swfs_api::swfs_u64;
use crate::fs::swfs::file::SwfsFile;
use crate::fs::swfs::mount_resources::SwfsWorkError;
use crate::fs::swfs::paths::RelativePathKey;
use crate::fs::swfs::paths::SwfsPath;
use crate::repository::RepositoryContext;
use crate::state::State;
use crate::state::StateError;
use crate::state::file_modified_time;
use crate::util::path::RelativePath;

/// What `state` holds at `path`, which is nothing rather than an error when it holds no such node.
pub async fn tracked_file_info(
    repository: Arc<RepositoryContext>,
    state: &Arc<State>,
    path: &RelativePath,
) -> Result<FileInfo, SwfsWorkError> {
    let node = match state.find_node(repository.clone(), path.as_str()).await {
        Err(StateError::NodeNotFound(_)) => return Ok(FileInfo::NotExist),
        found => found.internal("Unable to find file node")?,
    };
    if node.is_directory() {
        return Ok(FileInfo::Directory);
    }
    let mtime = file_modified_time(repository, path).await;
    Ok(FileInfo::from_node_and_mtime(&node, mtime))
}

/// Builds the SWFS files describing `written_paths` and the directories leading to them, as
/// `state` holds them.
///
/// A path `state` holds no node for was removed, and is described as holding nothing. Directory
/// entries are stamped with `directory_time`.
pub async fn written_files(
    files: &mut Vec<SwfsFile>,
    repository: Arc<RepositoryContext>,
    state: Arc<State>,
    written_paths: &DashSet<RelativePathKey>,
    directory_time: swfs_u64,
) -> Result<(), SwfsWorkError> {
    // Sorted, so a directory always precedes what it holds, which SWFS needs to see in that
    // order. Built before any await so no `DashSet` shard guard is held across one.
    let mut paths = BTreeSet::new();
    for written in written_paths.iter() {
        let mut path = written.0.as_str();
        while !path.is_empty() && paths.insert(path.to_owned()) {
            path = path.rsplit_once('/').map_or("", |(parent, _)| parent);
        }
    }

    for path in paths {
        let node = match state.find_node(repository.clone(), &path).await {
            Ok(node) => Some(node),
            Err(StateError::NodeNotFound(_)) => None,
            Err(err) => return Err(err.forward::<SwfsWorkError>("Unable to find written node")),
        };
        let relative = RelativePath::new_from_initial_path(&path)
            .forward::<SwfsWorkError>("Parsing written path")?;
        let file_name = SwfsPath::c_string_from_relative_path(&relative)
            .forward::<SwfsWorkError>("Parsing path")?;
        // `from_node_and_mtime` describes every node as a file, so a directory is named here.
        let info = match &node {
            Some(node) if node.is_directory() => FileInfo::Directory,
            Some(node) => FileInfo::from_node_and_mtime(node, 0),
            None => FileInfo::NotExist,
        };
        let mut file = SwfsFile::new(file_name, &info, None)
            .forward::<SwfsWorkError>("Creating SWFS file struct")?;
        if info.is_dir() {
            file.set_times(directory_time);
        }
        files.push(file);
    }

    Ok(())
}
