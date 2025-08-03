use cedar_policy::{PolicySet, Schema, Entities, Policy};
use etcd_client::{Client, EventType, Error as EtcdError, WatchOptions};
use rocket::futures::StreamExt;
use log::{debug, error, info, warn};
use rocket::serde::json::serde_json;
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
use std::str::FromStr;
use std::fmt::Debug;
use std::sync::Arc;

use crate::services::policies::PolicyStore;
use crate::services::data::DataStore;
use crate::services::schema::SchemaStore;
use crate::services::policies::memory::MemoryPolicyStore;
use crate::services::data::memory::MemoryDataStore;
use crate::services::schema::memory::MemorySchemaStore;
use crate::schemas::policies as policy_schemas;
use crate::schemas::data as data_schemas;
use crate::schemas::schema as schema_schemas;

/// Prefixes for etcd keys
const POLICY_PREFIX: &str = "cedar/policy/";
const SCHEMA_PREFIX: &str = "cedar/schema/";
const DATA_PREFIX: &str = "cedar/data/";

/// Configuration for the EtcdWatcher
#[derive(Debug, Clone)]
pub struct EtcdWatcherConfig {
    /// Etcd endpoints
    pub endpoints: Vec<String>,
    /// Reconnect delay in seconds when connection is lost
    pub reconnect_delay_secs: u64,
}

impl Default for EtcdWatcherConfig {
    fn default() -> Self {
        Self {
            endpoints: vec!["localhost:2379".to_string()],
            reconnect_delay_secs: 5,
        }
    }
}

/// EtcdWatcher watches etcd for changes to Cedar policies, schema, and data
pub struct EtcdWatcher {
    client: Client,
    policy_store: MemoryPolicyStore,
    schema_store: MemorySchemaStore,
    data_store: MemoryDataStore,
    config: EtcdWatcherConfig,
}

impl EtcdWatcher {
    /// Create a new EtcdWatcher
    pub async fn new(
        policy_store: MemoryPolicyStore,
        schema_store: MemorySchemaStore,
        data_store: MemoryDataStore,
        config: EtcdWatcherConfig,
    ) -> Result<Self, EtcdError> {
        let client = Client::connect(&config.endpoints, None).await?;
        Ok(Self {
            client,
            policy_store,
            schema_store,
            data_store,
            config,
        })
    }

    /// Start watching etcd for changes
    pub async fn start_watching(&self) -> Vec<JoinHandle<()>> {
        let mut handles = Vec::new();
        
        // Watch for policy changes
        let policy_handle = self.watch_policies(POLICY_PREFIX);
        handles.push(policy_handle);
        
        // Watch for schema changes
        let schema_handle = self.watch_schema(SCHEMA_PREFIX);
        handles.push(schema_handle);
        
        // Watch for data changes
        let data_handle = self.watch_data(DATA_PREFIX);
        handles.push(data_handle);
        
        handles
    }
    
    /// Watch for policy changes
    fn watch_policies(&self, prefix: &'static str) -> JoinHandle<()> {
        let endpoints = self.config.endpoints.clone();
        let reconnect_delay = self.config.reconnect_delay_secs;
        let policy_store = self.policy_store.clone();
        
        tokio::spawn(async move {
            loop {
                let result = Self::watch_policies_and_handle(endpoints.clone(), prefix, policy_store.clone()).await;
                if let Err(e) = result {
                    error!("Error watching etcd for policies: {}", e);
                    info!("Reconnecting to etcd in {} seconds", reconnect_delay);
                    sleep(Duration::from_secs(reconnect_delay)).await;
                }
            }
        })
    }
    
    /// Watch for schema changes
    fn watch_schema(&self, prefix: &'static str) -> JoinHandle<()> {
        let endpoints = self.config.endpoints.clone();
        let reconnect_delay = self.config.reconnect_delay_secs;
        let schema_store = self.schema_store.clone();
        
        tokio::spawn(async move {
            loop {
                let result = Self::watch_schema_and_handle(endpoints.clone(), prefix, schema_store.clone()).await;
                if let Err(e) = result {
                    error!("Error watching etcd for schema: {}", e);
                    info!("Reconnecting to etcd in {} seconds", reconnect_delay);
                    sleep(Duration::from_secs(reconnect_delay)).await;
                }
            }
        })
    }
    
    /// Watch for data changes
    fn watch_data(&self, prefix: &'static str) -> JoinHandle<()> {
        let endpoints = self.config.endpoints.clone();
        let reconnect_delay = self.config.reconnect_delay_secs;
        let data_store = self.data_store.clone();
        let schema_store = self.schema_store.clone();
        
        tokio::spawn(async move {
            loop {
                let result = Self::watch_data_and_handle(
                    endpoints.clone(), 
                    prefix, 
                    data_store.clone(),
                    schema_store.clone()
                ).await;
                if let Err(e) = result {
                    error!("Error watching etcd for data: {}", e);
                    info!("Reconnecting to etcd in {} seconds", reconnect_delay);
                    sleep(Duration::from_secs(reconnect_delay)).await;
                }
            }
        })
    }
    
    /// Watch etcd for policy changes and apply them
    async fn watch_policies_and_handle(
        endpoints: Vec<String>,
        prefix: &str,
        policy_store: MemoryPolicyStore,
    ) -> Result<(), String> {
        let mut client = match Client::connect(&endpoints, None).await {
            Ok(client) => client,
            Err(e) => return Err(format!("Failed to connect to etcd: {}", e)),
        };

        // First, load all existing policies
        Self::load_existing_policies(&mut client, prefix, &policy_store).await?;
        
        // Then start watching for changes
        let options = WatchOptions::new().with_prefix();
        let (_watcher, mut stream) = match client.watch(prefix, Some(options)).await {
            Ok((watcher, stream)) => (watcher, stream),
            Err(e) => return Err(format!("Failed to create watcher: {}", e)),
        };
        
        info!("Started watching etcd prefix for policies: {}", prefix);
        
        while let Some(resp) = stream.next().await {
            match resp {
                Ok(resp) => {
                    for event in resp.events() {
                        if let Some(kv) = event.kv() {
                            let key = match kv.key_str() {
                                Ok(k) => k,
                                Err(_) => continue,
                            };
                            
                            // Extract policy ID from the key (remove prefix)
                            let policy_id = key.trim_start_matches(prefix);
                            
                            match event.event_type() {
                                EventType::Put => {
                                    match kv.value_str() {
                                        Ok(value) => {
                                            debug!("Received policy update for ID: {}", policy_id);
                                            
                                            // Parse policy content
                                            match Self::parse_policy(policy_id, value) {
                                                Ok(policy) => {
                                                    // Get current schema for validation
                                                    let schema = match policy_store.get_policy(&policy_id).await {
                                                        Ok(_) => None, // For now, don't validate
                                                        Err(_) => None,
                                                    };
                                                    
                                                    // Update the policy
                                                    match policy_store.update_policy(
                                                        policy_id.to_string(),
                                                        policy_schemas::PolicyUpdate { content: policy.content },
                                                        schema,
                                                    ).await {
                                                        Ok(_) => info!("Updated policy {} from etcd", policy_id),
                                                        Err(e) => error!("Failed to update policy {}: {}", policy_id, e),
                                                    }
                                                },
                                                Err(e) => {
                                                    error!("Failed to parse policy from etcd: {}", e);
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            error!("Failed to read value for key {}: {}", key, e);
                                        }
                                    }
                                },
                                EventType::Delete => {
                                    warn!("Policy deletion detected for key: {}", key);
                                    match policy_store.delete_policy(policy_id).await {
                                        Ok(_) => info!("Deleted policy {} from store", policy_id),
                                        Err(e) => error!("Failed to delete policy {}: {}", policy_id, e),
                                    }
                                }
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("Watch error: {}", e);
                    return Err(format!("Watch error: {}", e));
                }
            }
        }
        
        Err("Watch stream ended unexpectedly".to_string())
    }
    
    /// Watch etcd for schema changes and apply them
    async fn watch_schema_and_handle(
        endpoints: Vec<String>,
        prefix: &str,
        schema_store: MemorySchemaStore,
    ) -> Result<(), String> {
        let mut client = match Client::connect(&endpoints, None).await {
            Ok(client) => client,
            Err(e) => return Err(format!("Failed to connect to etcd: {}", e)),
        };

        // First, load existing schema
        Self::load_existing_schema(&mut client, prefix, &schema_store).await?;
        
        // Then start watching for changes
        let options = WatchOptions::new().with_prefix();
        let (_watcher, mut stream) = match client.watch(prefix, Some(options)).await {
            Ok((watcher, stream)) => (watcher, stream),
            Err(e) => return Err(format!("Failed to create watcher: {}", e)),
        };
        
        info!("Started watching etcd prefix for schema: {}", prefix);
        
        while let Some(resp) = stream.next().await {
            match resp {
                Ok(resp) => {
                    for event in resp.events() {
                        if let Some(kv) = event.kv() {
                            let key = match kv.key_str() {
                                Ok(k) => k,
                                Err(_) => continue,
                            };

                            match event.event_type() {
                                EventType::Put => {
                                    match kv.value_str() {
                                        Ok(value) => {
                                            debug!("Received schema update");
                                            
                                            // Parse schema content
                                            match Self::parse_schema(value) {
                                                Ok(schema) => {
                                                    // Update the schema
                                                    match schema_store.update_schema(schema).await {
                                                        Ok(_) => info!("Updated schema from etcd"),
                                                        Err(e) => error!("Failed to update schema: {}", e),
                                                    }
                                                },
                                                Err(e) => {
                                                    error!("Failed to parse schema from etcd: {}", e);
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            error!("Failed to read value for key {}: {}", key, e);
                                        }
                                    }
                                },
                                EventType::Delete => {
                                    warn!("Schema deletion detected");
                                    schema_store.delete_schema().await;
                                    info!("Deleted schema from store");
                                }
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("Watch error: {}", e);
                    return Err(format!("Watch error: {}", e));
                }
            }
        }
        
        Err("Watch stream ended unexpectedly".to_string())
    }
    
    /// Watch etcd for data changes and apply them
    async fn watch_data_and_handle(
        endpoints: Vec<String>,
        prefix: &str,
        data_store: MemoryDataStore,
        schema_store: MemorySchemaStore,
    ) -> Result<(), String> {
        let mut client = match Client::connect(&endpoints, None).await {
            Ok(client) => client,
            Err(e) => return Err(format!("Failed to connect to etcd: {}", e)),
        };

        // First, load existing data
        Self::load_existing_data(&mut client, prefix, &data_store, &schema_store).await?;
        
        // Then start watching for changes
        let options = WatchOptions::new().with_prefix();
        let (_watcher, mut stream) = match client.watch(prefix, Some(options)).await {
            Ok((watcher, stream)) => (watcher, stream),
            Err(e) => return Err(format!("Failed to create watcher: {}", e)),
        };
        
        info!("Started watching etcd prefix for data: {}", prefix);
        
        while let Some(resp) = stream.next().await {
            match resp {
                Ok(resp) => {
                    for event in resp.events() {
                        if let Some(kv) = event.kv() {
                            let key = match kv.key_str() {
                                Ok(k) => k,
                                Err(_) => continue,
                            };

                            match event.event_type() {
                                EventType::Put => {
                                    match kv.value_str() {
                                        Ok(value) => {
                                            debug!("Received data update");
                                            
                                            // Parse data content
                                            match Self::parse_entities(value) {
                                                Ok(entities) => {
                                                    // Get current schema for validation
                                                    let schema = schema_store.get_cedar_schema().await;
                                                    
                                                    // Update the entities
                                                    match data_store.update_entities(entities, schema).await {
                                                        Ok(_) => info!("Updated entities from etcd"),
                                                        Err(e) => error!("Failed to update entities: {}", e),
                                                    }
                                                },
                                                Err(e) => {
                                                    error!("Failed to parse entities from etcd: {}", e);
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            error!("Failed to read value for key {}: {}", key, e);
                                        }
                                    }
                                },
                                EventType::Delete => {
                                    warn!("Data deletion detected");
                                    data_store.delete_entities().await;
                                    info!("Deleted entities from store");
                                }
                            }
                        }
                    }
                },
                Err(e) => {
                    error!("Watch error: {}", e);
                    return Err(format!("Watch error: {}", e));
                }
            }
        }
        
        Err("Watch stream ended unexpectedly".to_string())
    }
    
    /// Load existing policies from etcd
    async fn load_existing_policies(
        client: &mut Client,
        prefix: &str,
        policy_store: &MemoryPolicyStore,
    ) -> Result<(), String> {
        let options = etcd_client::GetOptions::new().with_prefix();
        let response = match client.get(prefix, Some(options)).await {
            Ok(resp) => resp,
            Err(e) => return Err(format!("Failed to get existing policies: {}", e)),
        };
        
        let mut policies = Vec::new();
        
        for kv in response.kvs() {
            if let (Ok(key), Ok(value)) = (kv.key_str(), kv.value_str()) {
                // Extract policy ID from the key
                let policy_id = key.trim_start_matches(prefix);
                debug!("Loading existing policy: {}", policy_id);
                
                match Self::parse_policy(policy_id, value) {
                    Ok(policy) => {
                        policies.push(policy);
                    },
                    Err(e) => {
                        error!("Failed to parse policy {}: {}", policy_id, e);
                    }
                }
            }
        }
        
        // Get schema for validation (optional)
        let schema = None; // No validation for now
        
        // Update all policies at once
        if !policies.is_empty() {
            match policy_store.update_policies(policies, schema).await {
                Ok(_) => info!("Loaded {} policies from etcd", response.kvs().len()),
                Err(e) => error!("Failed to load policies from etcd: {}", e),
            }
        }
        
        Ok(())
    }
    
    /// Load existing schema from etcd
    async fn load_existing_schema(
        client: &mut Client,
        prefix: &str,
        schema_store: &MemorySchemaStore,
    ) -> Result<(), String> {
        let options = etcd_client::GetOptions::new().with_prefix();
        let response = match client.get(prefix, Some(options)).await {
            Ok(resp) => resp,
            Err(e) => return Err(format!("Failed to get existing schema: {}", e)),
        };
        
        // We only expect one schema
        if let Some(kv) = response.kvs().first() {
            if let (Ok(_key), Ok(value)) = (kv.key_str(), kv.value_str()) {
                debug!("Loading existing schema");
                
                match Self::parse_schema(value) {
                    Ok(schema) => {
                        match schema_store.update_schema(schema).await {
                            Ok(_) => info!("Loaded schema from etcd"),
                            Err(e) => error!("Failed to load schema from etcd: {}", e),
                        }
                    },
                    Err(e) => {
                        error!("Failed to parse schema: {}", e);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Load existing data from etcd
    async fn load_existing_data(
        client: &mut Client,
        prefix: &str,
        data_store: &MemoryDataStore,
        schema_store: &MemorySchemaStore,
    ) -> Result<(), String> {
        let options = etcd_client::GetOptions::new().with_prefix();
        let response = match client.get(prefix, Some(options)).await {
            Ok(resp) => resp,
            Err(e) => return Err(format!("Failed to get existing data: {}", e)),
        };
        
        // We only expect one data entry
        if let Some(kv) = response.kvs().first() {
            if let (Ok(_key), Ok(value)) = (kv.key_str(), kv.value_str()) {
                debug!("Loading existing entities data");
                
                match Self::parse_entities(value) {
                    Ok(entities) => {
                        // Get schema for validation
                        let schema = schema_store.get_cedar_schema().await;
                        
                        match data_store.update_entities(entities, schema).await {
                            Ok(_) => info!("Loaded entities from etcd"),
                            Err(e) => error!("Failed to load entities from etcd: {}", e),
                        }
                    },
                    Err(e) => {
                        error!("Failed to parse entities: {}", e);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    // Parse methods stay the same
    fn parse_policy(id: &str, value: &str) -> Result<policy_schemas::Policy, String> {
        Ok(policy_schemas::Policy {
            id: id.to_string(),
            content: value.to_string(),
        })
    }
    
    fn parse_schema(value: &str) -> Result<schema_schemas::Schema, String> {
        match serde_json::from_str::<schema_schemas::Schema>(value) {
            Ok(schema) => Ok(schema),
            Err(e) => Err(format!("Failed to parse schema: {}", e)),
        }
    }
    
    fn parse_entities(value: &str) -> Result<data_schemas::Entities, String> {
        match serde_json::from_str::<data_schemas::Entities>(value) {
            Ok(entities) => Ok(entities),
            Err(e) => Err(format!("Failed to parse entities: {}", e)),
        }
    }
}