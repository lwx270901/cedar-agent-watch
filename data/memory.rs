use std::borrow::Borrow;
use std::error::Error;
use std::sync::Arc;

use async_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use async_trait::async_trait;
use cedar_policy::Schema;
use cedar_policy_core::entities;
use log::{debug, error, info};

use crate::schemas::data as schemas;
use crate::services::data::DataStore;

pub struct Entities(cedar_policy::Entities, entities::Entities);

impl Entities {
    fn empty() -> Self {
        Self {
            0: cedar_policy::Entities::empty(),
            1: entities::Entities::new(),
        }
    }

    fn cedar_entities(&self) -> cedar_policy::Entities {
        self.0.clone()
    }

    #[allow(dead_code)]
    fn core_entities(&self) -> entities::Entities {
        self.1.clone()
    }

    fn new(cedar_entities: cedar_policy::Entities, core_entities: entities::Entities) -> Self {
        Self {
            0: cedar_entities,
            1: core_entities,
        }
    }
}

// Inner implementation that holds the actual RwLock
struct MemoryDataStoreInner {
    entities: RwLock<Entities>,
}

impl MemoryDataStoreInner {
    fn new() -> Self {
        Self {
            entities: RwLock::new(Entities::empty()),
        }
    }

    async fn read(&self) -> RwLockReadGuard<Entities> {
        debug!("Trying to acquire read lock on entities");
        self.entities.read().await
    }

    async fn write(&self) -> RwLockWriteGuard<Entities> {
        debug!("Trying to acquire write lock on entities");
        self.entities.write().await
    }
}

// Outer store that can be cloned and shared
#[derive(Clone)]
pub struct MemoryDataStore {
    inner: Arc<MemoryDataStoreInner>,
}

impl MemoryDataStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MemoryDataStoreInner::new()),
        }
    }
    
    // Get a cloneable reference to this store
    pub fn get_shared_store(&self) -> Self {
        self.clone()
    }
}

#[async_trait]
impl DataStore for MemoryDataStore {
    async fn entities(&self) -> cedar_policy::Entities {
        let lock = self.inner.read().await;
        lock.cedar_entities()
    }

    async fn get_entities(&self) -> schemas::Entities {
        info!("Getting stored entities");
        let lock = self.inner.read().await;
        schemas::Entities::from(lock.1.clone())
    }

    async fn delete_entities(&self) {
        info!("Deleting stored entities");
        let mut lock = self.inner.write().await;
        *lock = Entities::empty();
    }

    async fn update_entities(
        &self,
        entities: schemas::Entities,
        schema: Option<Schema>,
    ) -> Result<schemas::Entities, Box<dyn Error>> {
        info!("Updating stored entities");
        let mut lock = self.inner.write().await;
        let core_entities: entities::Entities = match entities.try_into() {
            Ok(entities) => entities,
            Err(err) => {
                return {
                    error!("Failed to parse entities");
                    Err(err.into())
                }
            }
        };
        let schema_entities: schemas::Entities = core_entities.clone().into();
        let cedar_entities: cedar_policy::Entities = match schema_entities.borrow().convert_to_cedar_entities(&schema) {
            Ok(entities) => entities,
            Err(err) => return Err(err.into()),
        };
        *lock = Entities::new(cedar_entities, core_entities);
        Ok(schema_entities)
    }
}
