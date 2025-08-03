extern crate core;
extern crate rocket;

use std::borrow::Borrow;
use std::process::ExitCode;

use log::{error, info};
use rocket::catchers;
use rocket::http::ContentType;
use rocket_okapi::settings::UrlObject;
use rocket_okapi::{openapi_get_routes, rapidoc::*, swagger_ui::*};
use clap::Parser;

use crate::services::data::memory::MemoryDataStore;
use crate::services::data::DataStore;
use crate::services::policies::memory::MemoryPolicyStore;
use crate::services::policies::PolicyStore;
use crate::services::schema::memory::MemorySchemaStore;
use crate::services::schema::SchemaStore;

mod authn;
mod common;
mod config;
mod errors;
mod logger;
mod routes;
mod schemas;
mod services;
mod etcd_watcher;
use etcd_watcher::{EtcdWatcher, EtcdWatcherConfig};

// CLI args for etcd watcher configuration
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Etcd endpoints (comma separated)
    #[arg(long, default_value = "localhost:2379")]
    etcd_endpoints: String,
    
    /// Enable etcd watcher
    #[arg(long, default_value = "true")]
    enable_etcd_watcher: bool,
    
    /// Etcd reconnect delay in seconds
    #[arg(long, default_value = "5")]
    etcd_reconnect_delay: u64,
}

#[rocket::main]
async fn main() -> ExitCode {
    let config = config::init();
    logger::init(&config);
    let server_config: rocket::figment::Figment = config.borrow().into();

    // Parse CLI arguments
    let args = Args::parse();
    
    // Create our shared stores
    let policy_store = MemoryPolicyStore::new();
    let schema_store = MemorySchemaStore::new();
    let data_store = MemoryDataStore::new();
    
    // Create rocket instance with stores
    let mut rocket = rocket::custom(server_config)
        .attach(common::DefaultContentType::new(ContentType::JSON))
        .attach(services::schema::load_from_file::InitSchemaFairing)
        .attach(services::data::load_from_file::InitDataFairing)
        .attach(services::policies::load_from_file::InitPoliciesFairing)
        .manage(config)
        .manage(Box::new(policy_store.clone()) as Box<dyn PolicyStore>)
        .manage(Box::new(schema_store.clone()) as Box<dyn SchemaStore>)
        .manage(Box::new(data_store.clone()) as Box<dyn DataStore>)
        .manage(cedar_policy::Authorizer::new())
        .register(
            "/",
            catchers![
                errors::catchers::handle_500,
                errors::catchers::handle_404,
                errors::catchers::handle_400,
            ],
        )
        .mount("/", openapi_get_routes![routes::health,])
        .mount("/health", openapi_get_routes![routes::health,])
        .mount(
            "/v1",
            openapi_get_routes![
                routes::health,
                routes::policies::get_policies,
                routes::policies::get_policy,
                routes::policies::create_policy,
                routes::policies::update_policies,
                routes::policies::update_policy,
                routes::policies::delete_policy,
                routes::data::get_entities,
                routes::data::update_entities,
                routes::data::delete_entities,
                routes::authorization::is_authorized,
                routes::schema::get_schema,
                routes::schema::update_schema,
                routes::schema::delete_schema
            ],
        )
        .mount(
            "/swagger-ui/",
            make_swagger_ui(&SwaggerUIConfig {
                url: "../v1/openapi.json".to_owned(),
                ..Default::default()
            }),
        )
        .mount(
            "/rapidoc/",
            make_rapidoc(&RapiDocConfig {
                general: GeneralConfig {
                    spec_urls: vec![UrlObject::new("General", "../v1/openapi.json")],
                    ..Default::default()
                },
                hide_show: HideShowConfig {
                    allow_spec_url_load: false,
                    allow_spec_file_load: false,
                    ..Default::default()
                },
                ..Default::default()
            }),
        );

    // Start etcd watcher if enabled
    if args.enable_etcd_watcher {
        let endpoints = args.etcd_endpoints
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>();
        
        let watcher_config = EtcdWatcherConfig {
            endpoints,
            reconnect_delay_secs: args.etcd_reconnect_delay,
        };
        
        // Pass the concrete store types directly to the watcher
        match EtcdWatcher::new(
            policy_store, 
            schema_store,
            data_store,
            watcher_config
        ).await {
            Ok(watcher) => {
                info!("Starting etcd watcher");
                let _handles = watcher.start_watching().await;
            }
            Err(e) => {
                error!("Failed to initialize etcd watcher: {}", e);
            }
        }
    }

    // Launch rocket
    let launch_result = rocket.launch().await;
    match launch_result {
        Ok(_) => {
            info!("Cedar-Agent shut down gracefully.");
            ExitCode::SUCCESS
        }
        Err(err) => {
            error!("Cedar-Agent shut down with error: {}", err);
            ExitCode::FAILURE
        }
    }
}