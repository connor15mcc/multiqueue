use crate::api::AppState;
use crate::api::handlers;
use axum::{Router, routing::post};
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

/// Create all routes for the application
pub fn create_routes(state: AppState) -> Router {
    // CORS configuration
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Create a router with all routes
    Router::new()
        // Task submission endpoint
        .route("/tasks/submit", post(handlers::submit_task))
        .route("/tasks/view", post(handlers::view_tasks))
        .route("/tasks/stats", post(handlers::get_stats))
        .route("/tasks/cancel", post(handlers::cancel_tasks))
        // Add CORS and tracing
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        // Add shared state
        .with_state(state)
}
