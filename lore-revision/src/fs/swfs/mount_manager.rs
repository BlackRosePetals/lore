use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use lore_base::error::InvalidPath;
use lore_error_set::ForwardStrict;
use lore_error_set::error_set;

use crate::fs::swfs::api_interface::SwfsInterface;
use crate::fs::swfs::api_interface::WrappedHandle;
use crate::fs::swfs::api_interface::swfs_api::SWFSHandle;
use crate::fs::swfs::filesystem::SwfsFilesystem;
use crate::fs::swfs::mount_resources::MountResources;
use crate::fs::swfs::paths::MountPath;
use crate::global::external_dir::check_for_external_lore_dir;
use crate::global::external_dir::external_dir_for_instance;
use crate::instance::InstanceId;
use crate::instance::list_instances;
use crate::lore::RepositoryId;
use crate::lore_warn;
use crate::repository::DOT_LORE;
use crate::repository::RepositoryConfig;
use crate::repository::load_repository_config_from_dot_dir;
use crate::shared_store::registry::SharedStoreRegistry;
use crate::shared_store::registry::SharedStoreRegistryEntry;
use crate::util::config::SaveableConfig;

pub struct Mount {
    instance_id: InstanceId,
    mount: Arc<SwfsInterface>,
}

#[error_set]
pub enum MountManagerError {
    InvalidPath,
}

pub struct MountManager {
    mounted_paths: DashMap<MountPath, Mount>,
    swfs_id_lookup: DashMap<WrappedHandle, Arc<SwfsInterface>>,
}

impl MountManager {
    /// Mounts every SWFS instance of the stores in `entries`. A store that cannot be opened or
    /// listed, and an instance whose config cannot be read, are logged and skipped rather than
    /// stopping the rest.
    #[lore_macro::test_pub]
    async fn mounts_of_entries(
        entries: &[SharedStoreRegistryEntry],
    ) -> Result<DashMap<MountPath, Mount>, MountManagerError> {
        let mounted_paths = DashMap::new();
        for entry in entries {
            let Ok(entry_repository) =
                entry
                    .create_null_repository_context()
                    .await
                    .inspect_err(|error| {
                        lore_warn!("Unable to open shared store at {}: {error}", entry.path());
                    })
            else {
                continue;
            };
            let Ok(instances) = list_instances(&entry_repository)
                .await
                .inspect_err(|error| {
                    lore_warn!(
                        "Unable to list instances of shared store at {}: {error}",
                        entry.path()
                    );
                })
            else {
                continue;
            };
            for instance in &instances {
                let Some(dot_lore) =
                    check_for_external_lore_dir(instance.instance_id)
                        .forward::<MountManagerError>("Unable to find external .lore directory")?
                else {
                    continue;
                };
                let Ok(config) =
                    load_repository_config_from_dot_dir(&dot_lore).inspect_err(|error| {
                        lore_warn!(
                            "Unable to load the repository config of instance {} at {}: {error}",
                            instance.instance_id,
                            instance.path
                        );
                    })
                else {
                    continue;
                };
                if config.is_swfs() {
                    let mount_path = MountPath::new(Path::new(&instance.path))
                        .forward::<MountManagerError>("Failed to make mount path")?;
                    let mount = SwfsInterface::mount(
                        MountResources::premount_instance(instance.instance_id, mount_path.clone())
                            .await
                            .forward::<MountManagerError>("Failed gathering premount resources")?,
                    )
                    .forward::<MountManagerError>("Failed to mount")?;

                    mounted_paths.insert(
                        mount_path,
                        Mount {
                            instance_id: instance.instance_id,
                            mount,
                        },
                    );
                }
            }
        }
        Ok(mounted_paths)
    }

    pub async fn initialize() -> Result<Arc<Self>, MountManagerError> {
        let registry = SharedStoreRegistry::load()
            .await
            .forward::<MountManagerError>("Unable to load shared store registry")?;
        let mounted_paths = Self::mounts_of_entries(registry.entries()).await?;
        let lookup = DashMap::new();
        for mount in mounted_paths.iter() {
            mount
                .mount
                .wait_for_ready()
                .await
                .forward::<MountManagerError>("Failed waiting for mount to be ready")?;
            let handle = mount
                .mount
                .handle()
                .forward::<MountManagerError>("Error getting mount handle after being readier")?;
            lookup.insert(handle, mount.mount.clone());
        }
        Ok(Arc::new(MountManager {
            mounted_paths,
            swfs_id_lookup: lookup,
        }))
    }

    pub fn check_for_external_lore_dir(
        &self,
        repository_path: &Path,
    ) -> Result<Option<PathBuf>, MountManagerError> {
        let mount_path = MountPath::new(repository_path)
            .forward::<MountManagerError>("Failed to make mount path")?;
        if let Some(mount) = self.mounted_paths.get(&mount_path) {
            check_for_external_lore_dir(mount.instance_id).forward::<MountManagerError>(
                "Unable to find external lore directory for expected instance ID",
            )
        } else {
            Ok(None)
        }
    }

    pub fn get_mount_filesystem_provider(
        &self,
        repository_path: &Path,
    ) -> Result<Arc<SwfsFilesystem>, MountManagerError> {
        let mount_path = MountPath::new(repository_path)
            .forward::<MountManagerError>("Failed to make mount path")?;
        let mount = self
            .mounted_paths
            .get(&mount_path)
            .ok_or_else(|| InvalidPath {
                path: mount_path.as_ref().display().to_string(),
            })?;
        Ok(Arc::new(SwfsFilesystem::new(
            mount_path.as_ref(),
            mount.mount.clone(),
        )))
    }

    pub async fn create_mount(
        &self,
        repository_path: &Path,
        repository_config: &RepositoryConfig,
        repository_id: RepositoryId,
        instance_id: InstanceId,
    ) -> Result<PathBuf, MountManagerError> {
        let mount_path = MountPath::new(repository_path)
            .forward::<MountManagerError>("Failed to make mount path")?;
        if let Some(_mount) = self.mounted_paths.get(&mount_path) {
            Err(MountManagerError::internal(format!(
                "Mount already exists for path {}",
                repository_path.display()
            )))
        } else {
            let dot_lore = external_dir_for_instance(instance_id)
                .forward::<MountManagerError>("Unable to get external directory")?
                .join(DOT_LORE);
            let mount = SwfsInterface::mount(
                MountResources::premount_instance_with_config(
                    repository_id,
                    instance_id,
                    mount_path.clone(),
                    repository_config,
                )
                .await
                .forward::<MountManagerError>("Failed gathering premount resources")?,
            )
            .forward::<MountManagerError>("Failed to mount")?;
            let existing = self.mounted_paths.insert(
                mount_path,
                Mount {
                    instance_id,
                    mount: mount.clone(),
                },
            );
            if let Some(_existing) = existing {
                return Err(MountManagerError::internal(format!(
                    "Mount double created for path {}",
                    repository_path.display()
                )));
            }
            self.swfs_id_lookup.insert(
                mount
                    .handle()
                    .forward::<MountManagerError>("Unable to get handle for fresh mount")?,
                mount.clone(),
            );
            mount
                .wait_for_ready()
                .await
                .forward::<MountManagerError>("Failed waiting for mount to be ready")?;
            Ok(dot_lore)
        }
    }

    pub fn get_interface_from_swfs_handle(&self, handle: SWFSHandle) -> Option<Arc<SwfsInterface>> {
        self.swfs_id_lookup
            .get(&WrappedHandle::new(handle))
            .map(|entry| entry.value().clone())
    }

    /// Returns the number of currently mounted SWFS paths.
    pub fn mount_count(&self) -> usize {
        self.mounted_paths.len()
    }
}
