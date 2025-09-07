//! HTTP API for the multiqueue service
//!
//! This module provides HTTP endpoints that map to the protobuf service definitions.

pub mod handlers;
pub mod routes;

use crate::multiqueue::MultiQueue;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Shared state that will be available to all request handlers
#[derive(Clone)]
pub struct AppState {
    /// Thread-safe reference to the MultiQueue
    pub multiqueue: Arc<Mutex<MultiQueue>>,
}

impl AppState {
    /// Create a new AppState with the given MultiQueue
    pub fn new(multiqueue: MultiQueue) -> Self {
        Self {
            multiqueue: Arc::new(Mutex::new(multiqueue)),
        }
    }
}

/// Start the HTTP server
pub async fn serve(state: AppState, addr: &str) -> Result<(), anyhow::Error> {
    let app = routes::create_routes(state);

    // Parse the address
    let addr = SocketAddr::from_str(addr)
        .map_err(|e| anyhow::anyhow!("Failed to parse address: {}", e))?;

    // Start the server
    tracing::info!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

