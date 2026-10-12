// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::path::Path;
use std::sync::Arc;

use lore_revision::fs::filesystem_provider::FilesystemProvider;
use lore_revision::instance::InstanceId;
use lore_revision::node::Node;
use lore_revision::node::NodeFlags;
use lore_revision::repository::DOT_LORE;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::RepositoryPaths;
use lore_revision::repository::RepositoryWriteToken;
use lore_storage::hash;
use zerocopy::FromZeros;

pub fn default_repository_creation_args(
    immutable_store: std::sync::Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: std::sync::Arc<dyn lore_storage::MutableStore>,
) -> lore_revision::repository::RepositoryContextCreationArgs {
    lore_revision::repository::RepositoryContextCreationArgs {
        paths: None,
        immutable_store,
        mutable_store,
        id: lore_base::types::Context::from(uuid::Uuid::now_v7()).into(),
        instance_id: InstanceId::generate(),
        remote: Err(lore_transport::ProtocolError::from(
            lore_base::error::NoRemote,
        )),
        filter: std::sync::Arc::default(),
        filesystem_provider: None,
    }
}

pub trait RepositoryContextCreationArgsExt {
    fn with_path(self, path: impl AsRef<std::path::Path>) -> Self;
    fn with_id(self, id: lore_revision::lore::RepositoryId) -> Self;
    fn with_instance_id(self, id: lore_revision::instance::InstanceId) -> Self;
    fn with_filter(self, filter: std::sync::Arc<lore_revision::filter::Filter>) -> Self;
    fn with_filesystem_provider(self, filesystem_provider: Arc<dyn FilesystemProvider>) -> Self;
}

impl RepositoryContextCreationArgsExt for lore_revision::repository::RepositoryContextCreationArgs {
    fn with_path(mut self, path: impl AsRef<std::path::Path>) -> Self {
        self.paths = Some(RepositoryPaths::new(
            path.as_ref().to_owned(),
            path.as_ref().join(DOT_LORE),
        ));
        self
    }

    fn with_id(mut self, id: lore_revision::lore::RepositoryId) -> Self {
        self.id = id;
        self
    }

    fn with_instance_id(mut self, id: lore_revision::instance::InstanceId) -> Self {
        self.instance_id = id;
        self
    }

    fn with_filter(mut self, filter: std::sync::Arc<lore_revision::filter::Filter>) -> Self {
        self.filter = filter;
        self
    }

    fn with_filesystem_provider(
        mut self,
        filesystem_provider: Arc<dyn FilesystemProvider>,
    ) -> Self {
        self.filesystem_provider = Some(filesystem_provider);
        self
    }
}

/// A repository over in-memory stores and no remote, writing its working tree at `path`.
pub async fn working_tree_repository(path: &Path) -> Arc<RepositoryContext> {
    let immutable_store = lore_storage::local::immutable_store::create(
        None::<&str>,
        lore_storage::local::immutable_store::ImmutableStoreCreateOptions::none(),
        false,
        lore_storage::ImmutableStoreSettings::default(),
    )
    .await
    .expect("in-memory immutable store");
    let mutable_store = lore_storage::local::mutable_store::create(
        None::<&str>,
        lore_storage::MutableStoreSettings::default(),
        immutable_store.clone(),
    )
    .await
    .expect("in-memory mutable store");

    Arc::new(
        RepositoryContext::new(
            default_repository_creation_args(immutable_store, mutable_store).with_path(path),
        )
        .with_write_token(RepositoryWriteToken::acquire(path).await),
    )
}

/// A file node addressing `content`, which no store holds until it is written to one.
pub fn file_node(content: &[u8]) -> Node {
    let mut node = Node::new_zeroed();
    node.flags = NodeFlags::File.bits();
    node.address = lore_base::types::Address::zero_context_hash(hash::hash_slice(content));
    node.size = content.len() as u64;
    node
}
