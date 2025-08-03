use std::time::Duration;

// Import ALL necessary traits to use their methods
use cedar_agent::services::policies::PolicyStore;
use cedar_agent::services::schema::SchemaStore;
use cedar_agent::services::data::DataStore;

use cedar_agent::services::policies::memory::MemoryPolicyStore;
use cedar_agent::services::schema::memory::MemorySchemaStore;
use cedar_agent::services::data::memory::MemoryDataStore;
use cedar_agent::etcd_watcher::{EtcdWatcher, EtcdWatcherConfig};

// Use the etcd_client crate directly:
use etcd_client::Client;

#[tokio::test]
async fn test_etcd_watcher_policy_sync() {
    // 1. Setup: create shared stores
    let policy_store = MemoryPolicyStore::new();
    let schema_store = MemorySchemaStore::new();
    let data_store = MemoryDataStore::new();

    // 2. Start the watcher (it will spawn its own tasks)
    let watcher_config = EtcdWatcherConfig {
        endpoints: vec!["localhost:2379".to_string()],
        reconnect_delay_secs: 1,
    };
    let watcher = EtcdWatcher::new(
        policy_store.clone(),
        schema_store.clone(),
        data_store.clone(),
        watcher_config,
    )
    .await
    .expect("failed to start watcher");
    let _handles = watcher.start_watching().await;

    // 3. Write a policy to etcd
    let mut client = Client::connect(["localhost:2379"], None)
        .await
        .expect("failed to connect to etcd");
    let policy_id = "test_policy";
    // Store just the policy content, not JSON (to fix your parsing error)
    let policy_content = "permit(principal, action, resource);";
    client
        .put(
            format!("cedar/policy/{}", policy_id),
            policy_content,
            None,
        )
        .await
        .expect("failed to put policy in etcd");

    // 4. Wait for the watcher to pick up the change
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 5. Assert: The policy store should now have the policy
    let stored_policy = policy_store.get_policy(policy_id).await.expect("policy not found");
    assert_eq!(stored_policy.id, policy_id);
    assert_eq!(stored_policy.content, policy_content);

    // 6. Clean up: remove the policy from etcd
    let _ = client.delete(format!("cedar/policy/{}", policy_id), None).await;
}

#[tokio::test]
async fn test_etcd_watcher_schema_sync() {
    // Similar setup to policy test
    let policy_store = MemoryPolicyStore::new();
    let schema_store = MemorySchemaStore::new();
    let data_store = MemoryDataStore::new();
    
    let watcher_config = EtcdWatcherConfig {
        endpoints: vec!["localhost:2379".to_string()],
        reconnect_delay_secs: 1,
    };
    
    let watcher = EtcdWatcher::new(
        policy_store.clone(),
        schema_store.clone(),
        data_store.clone(),
        watcher_config,
    )
    .await
    .expect("failed to start watcher");
    
    let _handles = watcher.start_watching().await;
    
    // Connect to etcd
    let mut client = Client::connect(["localhost:2379"], None)
        .await
        .expect("failed to connect to etcd");
    
    // Create a simple schema JSON
    let schema_json = r#"
    {
        "": {
            "entityTypes": {
                "User": {
                    "shape": {
                        "type": "Record",
                        "attributes": {}
                    }
                }
            }
        }
    }
    "#;
    
    // Put the schema in etcd
    client
        .put("cedar/schema/main_schema", schema_json, None)
        .await
        .expect("failed to put schema in etcd");
    
    // Wait for the watcher to process
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Check that schema was updated in store
    let stored_schema = schema_store.get_internal_schema().await;
    assert!(!stored_schema.is_empty(), "Schema should not be empty after update");
    
    // Clean up
    let _ = client.delete("cedar/schema/main_schema", None).await;
}

#[tokio::test]
async fn test_etcd_watcher_data_sync() {
    // Similar setup to other tests
    let policy_store = MemoryPolicyStore::new();
    let schema_store = MemorySchemaStore::new();
    let data_store = MemoryDataStore::new();
    
    let watcher_config = EtcdWatcherConfig {
        endpoints: vec!["localhost:2379".to_string()],
        reconnect_delay_secs: 1,
    };
    
    let watcher = EtcdWatcher::new(
        policy_store.clone(),
        schema_store.clone(),
        data_store.clone(),
        watcher_config,
    )
    .await
    .expect("failed to start watcher");
    
    let _handles = watcher.start_watching().await;
    
    // Connect to etcd
    let mut client = Client::connect(["localhost:2379"], None)
        .await
        .expect("failed to connect to etcd");
    
    // Create sample entity data
    let entities_json = r#"
    {
        "entities": [
            {
                "uid": {
                    "type": "User",
                    "id": "alice"
                },
                "attrs": {},
                "parents": []
            }
        ]
    }
    "#;
    
    // Put the entities in etcd
    client
        .put("cedar/data/main_entities", entities_json, None)
        .await
        .expect("failed to put entities in etcd");
    
    // Wait for the watcher to process
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Check that entities were updated in store
    let stored_entities = data_store.get_entities().await;
    assert!(stored_entities.len() > 0, "Entities should be present after update");
    
    // Clean up
    let _ = client.delete("cedar/data/main_entities", None).await;
}