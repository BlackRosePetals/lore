// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_revision::change;
use lore_revision::change::FileAction;
use lore_revision::change::NodeChange;
use lore_revision::change::NodeChangeState;
use lore_revision::commit::commit_in_memory_revision;
use lore_revision::lore::BranchId;
use lore_revision::metadata::Metadata;
use lore_revision::node::*;
use lore_revision::repository::InMemoryContext;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::RepositoryWriteToken;
use lore_revision::revision::find_last_merged_revision;
use lore_revision::state::NodeMapping;
use lore_revision::state::State;
use lore_revision::util::path::RelativePathBuf;
use lore_storage::hash::hash_string;

use crate::helper::default_repository_creation_args;
use crate::helper::test_store_create;

struct InMemoryMarker;
impl InMemoryContext for InMemoryMarker {}
const IN_MEMORY_MARKER: InMemoryMarker = InMemoryMarker;

fn token() -> RepositoryWriteToken {
    RepositoryWriteToken::in_memory(&IN_MEMORY_MARKER)
}

/// A path-less repository on the stores.
fn in_memory_repository(
    immutable: Arc<dyn lore_storage::ImmutableStore>,
    mutable: Arc<dyn lore_storage::MutableStore>,
) -> Arc<RepositoryContext> {
    Arc::new(
        RepositoryContext::new(default_repository_creation_args(immutable, mutable))
            .with_write_token(token()),
    )
}

fn file(name: &str) -> Node {
    Node {
        flags: NodeFlags::File.bits(),
        mode: 0o644,
        size: 10,
        address: Address {
            hash: Hash::from_u64(0x5a),
            context: Context::from(uuid::Uuid::now_v7()),
        },
        name_hash: hash_string(name),
        ..Default::default()
    }
}

fn branch() -> BranchId {
    Context::from(uuid::Uuid::now_v7())
}

/// Adds `node` as `name` at the root, staged as an add.
async fn add(state: &State, repository: &Arc<RepositoryContext>, node: Node, name: &str) -> NodeID {
    let node_id = state
        .node_add(repository.clone(), ROOT_NODE, node, name)
        .await
        .expect("adding the node");
    state
        .node_mark_staged(
            repository.clone(),
            node_id,
            NodeFlags::StagedAdd,
            NodeFlags::DirtyAdd,
        )
        .await
        .expect("staging the add");
    node_id
}

/// Commits `state` on top of `current` on `branch`.
async fn commit(
    repository: &Arc<RepositoryContext>,
    state: &Arc<State>,
    current: Hash,
    branch: BranchId,
) -> Hash {
    let mut metadata = Metadata::new();
    metadata.set_branch(branch).expect("setting the branch");
    commit_in_memory_revision(
        repository.clone(),
        &token(),
        state.clone(),
        metadata,
        current,
        branch,
    )
    .await
    .expect("committing the revision")
}

async fn revision_state(repository: &Arc<RepositoryContext>, revision: Hash) -> Arc<State> {
    State::deserialize(repository.clone(), revision)
        .await
        .expect("deserializing the revision")
}

/// Sets the history of `node_id` in `state` to `first` along the first parent and `second` along
/// the second, each a revision and the file's node ID there, both as modifications, and serializes
/// `state`.
async fn with_history(
    repository: &Arc<RepositoryContext>,
    state: &Arc<State>,
    node_id: NodeID,
    first: (Hash, NodeID),
    second: (Hash, NodeID),
) -> Hash {
    let metadata_node = node_to_file_metadata(node_id);
    let block_index = NodeFileMetadataBlock::index(metadata_node);
    let block = state
        .block_file_metadata(repository.clone(), block_index)
        .await
        .expect("reading the file metadata block");
    let dirtied = {
        let mut writer = block.write();
        let history = writer.node(NodeFileMetadata::index(metadata_node));
        history.revision = [first.0, second.0];
        history.node = [first.1, second.1];
        history.action = [FileAction::Keep as u16; 2];
        history.flags = [0; 2];
        writer.mark_dirty()
    };
    if dirtied {
        state.block_file_metadata_modified(block, block_index);
        state.mark_dirty();
    }
    state
        .serialize(repository.clone(), &token())
        .await
        .expect("serializing the revision")
}

/// A change whose target side is `node_id` of `state`, at `path`.
fn change_at(
    repository: &Arc<RepositoryContext>,
    state: &Arc<State>,
    node_id: NodeID,
    path: &str,
) -> NodeChange {
    let side = NodeChangeState {
        mapping: NodeMapping {
            repository: repository.clone(),
            state: state.clone(),
            path: RelativePathBuf::new().push_and_freeze(path),
            node: node_id,
        },
        observed: None,
        flags: NodeFlags::File,
        address: Address::default(),
        mode: 0,
    };
    NodeChange {
        action: FileAction::Keep,
        flags: change::Flags::None,
        from: side.clone(),
        to: side,
    }
}

/// The target branch merged `shared.txt` from the source branch in a revision that gives the file
/// another node ID than the merge of divergent history after it. The walk reads that revision's
/// history at the node ID the merge's history records for the file there, and finds the merge from
/// the source branch.
#[tokio::test]
async fn the_last_merged_revision_is_found_past_a_merge_that_gives_the_file_another_node_id() {
    let (immutable, mutable, execution) = test_store_create().await.expect("creating the stores");
    lore_spawn!(LORE_CONTEXT.scope(execution, async move {
        let repository = in_memory_repository(immutable, mutable);
        let target = branch();
        let source = branch();
        let local = branch();

        let state = State::new();
        add(&state, &repository, file("base.txt"), "base.txt").await;
        let branch_point = commit(&repository, &state, Hash::default(), target).await;

        let shared = file("shared.txt");
        let source_state = revision_state(&repository, branch_point).await;
        let shared_in_source = add(&source_state, &repository, shared, "shared.txt").await;
        let merged_from_source = commit(&repository, &source_state, branch_point, source).await;

        let remote = revision_state(&repository, branch_point).await;
        add(&remote, &repository, file("earlier.txt"), "earlier.txt").await;
        let shared_in_remote = add(&remote, &repository, shared, "shared.txt").await;
        commit(&repository, &remote, branch_point, target).await;
        remote.set_parent_other(merged_from_source);
        let remote_latest = with_history(
            &repository,
            &remote,
            shared_in_remote,
            (Hash::default(), INVALID_NODE),
            (merged_from_source, shared_in_source),
        )
        .await;

        let local_state = revision_state(&repository, branch_point).await;
        let shared_in_local = add(&local_state, &repository, shared, "shared.txt").await;
        let local_latest = commit(&repository, &local_state, branch_point, local).await;
        assert_ne!(shared_in_local, shared_in_remote);

        let merge = revision_state(&repository, local_latest).await;
        add(&merge, &repository, file("merge.txt"), "merge.txt").await;
        let merged = commit(&repository, &merge, local_latest, local).await;
        let merge = revision_state(&repository, merged).await;
        merge.set_parent_self(remote_latest);
        merge.set_parent_other(local_latest);
        let merge_revision = with_history(
            &repository,
            &merge,
            shared_in_local,
            (remote_latest, shared_in_remote),
            (local_latest, shared_in_local),
        )
        .await;

        let merge = revision_state(&repository, merge_revision).await;
        let state_branch_point = revision_state(&repository, branch_point).await;
        let found = find_last_merged_revision(
            &change_at(&repository, &merge, shared_in_local, "shared.txt"),
            source,
            target,
            state_branch_point,
        )
        .await
        .expect("walking the history");

        let source_revision = revision_state(&repository, merged_from_source).await;
        assert_eq!(
            found,
            Some((merged_from_source, source_revision.revision_number()))
        );
    }))
    .await
    .expect("Task failed");
}

/// The source branch revision `old.txt` was added in, and its node there, on top of
/// `branch_point`.
async fn source_add(
    repository: &Arc<RepositoryContext>,
    branch_point: Hash,
    source: BranchId,
) -> (Hash, NodeID) {
    let state = revision_state(repository, branch_point).await;
    let node_id = add(&state, repository, file("old.txt"), "old.txt").await;
    (
        commit(repository, &state, branch_point, source).await,
        node_id,
    )
}

/// The target branch adds `new.txt` in the revision the walk starts from, at a node whose history
/// still names a merge of another file from the source branch. The file has no history before
/// that revision, so the walk finds no merge.
#[tokio::test]
async fn a_file_the_target_adds_in_the_start_revision_has_no_last_merge() {
    let (immutable, mutable, execution) = test_store_create().await.expect("creating the stores");
    lore_spawn!(LORE_CONTEXT.scope(execution, async move {
        let repository = in_memory_repository(immutable, mutable);
        let target = branch();
        let source = branch();

        let state = State::new();
        add(&state, &repository, file("base.txt"), "base.txt").await;
        let branch_point = commit(&repository, &state, Hash::default(), target).await;
        let (merged_from_source, old_in_source) =
            source_add(&repository, branch_point, source).await;

        let state = revision_state(&repository, branch_point).await;
        let added = add(&state, &repository, file("new.txt"), "new.txt").await;
        let adding = commit(&repository, &state, branch_point, target).await;
        let start = with_history(
            &repository,
            &revision_state(&repository, adding).await,
            added,
            (branch_point, added),
            (merged_from_source, old_in_source),
        )
        .await;

        let start = revision_state(&repository, start).await;
        let found = find_last_merged_revision(
            &change_at(&repository, &start, added, "new.txt"),
            source,
            target,
            revision_state(&repository, branch_point).await,
        )
        .await
        .expect("walking the history");

        assert_eq!(found, None);
    }))
    .await
    .expect("Task failed");
}

/// The target branch adds `new.txt` at a node whose history still names a merge of another file
/// from the source branch, and changes another file after it. The walk stops at the revision that
/// added the file and finds no merge.
#[tokio::test]
async fn the_walk_stops_at_the_revision_the_target_added_the_file_in() {
    let (immutable, mutable, execution) = test_store_create().await.expect("creating the stores");
    lore_spawn!(LORE_CONTEXT.scope(execution, async move {
        let repository = in_memory_repository(immutable, mutable);
        let target = branch();
        let source = branch();

        let state = State::new();
        add(&state, &repository, file("base.txt"), "base.txt").await;
        let branch_point = commit(&repository, &state, Hash::default(), target).await;
        let (merged_from_source, old_in_source) =
            source_add(&repository, branch_point, source).await;

        let state = revision_state(&repository, branch_point).await;
        let added = add(&state, &repository, file("new.txt"), "new.txt").await;
        let adding = commit(&repository, &state, branch_point, target).await;
        let adding = with_history(
            &repository,
            &revision_state(&repository, adding).await,
            added,
            (branch_point, added),
            (merged_from_source, old_in_source),
        )
        .await;

        let state = revision_state(&repository, adding).await;
        add(&state, &repository, file("later.txt"), "later.txt").await;
        let latest = commit(&repository, &state, adding, branch()).await;

        let latest = revision_state(&repository, latest).await;
        let found = find_last_merged_revision(
            &change_at(&repository, &latest, added, "new.txt"),
            source,
            target,
            revision_state(&repository, branch_point).await,
        )
        .await
        .expect("walking the history");

        assert_eq!(found, None);
    }))
    .await
    .expect("Task failed");
}
