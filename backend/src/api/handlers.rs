use crate::api::AppState;
use crate::proto::multiqueue::cancel_tasks_request::selector::Selector;
use crate::proto::multiqueue::{
    CancelTasksRequest, CancelTasksResponse, GetStatsRequest, GetStatsResponse, SubmitTaskRequest,
    SubmitTaskResponse, ViewTasksRequest, ViewTasksResponse, cancel_tasks_response,
    get_stats_response, submit_task_response, view_tasks_response,
};
use crate::tasks::{Task, TaskState, Tier};
use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use tracing::info;

/// Error types for API handlers
pub enum ApiError {
    /// Error from the multiqueue
    QueueError(anyhow::Error),
    /// Invalid request data
    BadRequest(String),
    /// Task not found
    NotFound(String),
}

impl From<anyhow::Error> for ApiError {
    fn from(err: anyhow::Error) -> Self {
        Self::QueueError(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            ApiError::QueueError(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
        };

        let body = Json(json!({
            "error": error_message,
        }));

        (status, body).into_response()
    }
}

#[axum::debug_handler]
pub async fn submit_task(
    State(state): State<AppState>,
    Json(request): Json<SubmitTaskRequest>,
) -> Result<Json<SubmitTaskResponse>, ApiError> {
    info!("Recv request: {request:?}");
    let task = Task::try_from(request).map_err(|err| ApiError::BadRequest(err.to_string()))?;

    let mut multiqueue = state.multiqueue.lock().await;
    let count = multiqueue.insert(vec![task.clone()]).await?;
    if count == 0 {
        return Err(ApiError::BadRequest("could not be inserted".to_string()));
    }

    let response = SubmitTaskResponse {
        response: Some(submit_task_response::Response::Success(task.into())),
    };
    Ok(Json(response))
}

#[axum::debug_handler]
pub async fn view_tasks(
    State(state): State<AppState>,
    Json(request): Json<ViewTasksRequest>,
) -> Result<Json<ViewTasksResponse>, ApiError> {
    let limit = request.limit.unwrap_or(20);
    let state_filter = TaskState::from(request.state_filter());

    let mut multiqueue = state.multiqueue.lock().await;
    let tasks = multiqueue.view_by_state(state_filter, Some(limit)).await?;

    let task_list = view_tasks_response::TaskList {
        tasks: tasks.into_iter().map(|t| t.into()).collect(),
    };
    let response = ViewTasksResponse {
        response: Some(view_tasks_response::Response::Success(task_list)),
    };
    Ok(Json(response))
}

#[axum::debug_handler]
pub async fn get_stats(
    State(state): State<AppState>,
    request: Json<GetStatsRequest>,
) -> Result<Json<GetStatsResponse>, ApiError> {
    let tier = match request.tier {
        Some(tier) => Some(Tier::try_from(tier)?),
        None => None,
    };
    let mut multiqueue = state.multiqueue.lock().await;

    // Get counts for each state
    let waiting_count = multiqueue.count(TaskState::Waiting, tier).await?;
    let queued_count = multiqueue.count(TaskState::Queued, tier).await?;
    let complete_count = multiqueue.count(TaskState::Complete, tier).await?;
    let cancelled_count = multiqueue.count(TaskState::Cancelled, tier).await?;
    let total_count = waiting_count + queued_count + complete_count + cancelled_count;

    let stats = get_stats_response::Stats {
        waiting_count: waiting_count as i64,
        queued_count: queued_count as i64,
        complete_count: complete_count as i64,
        cancelled_count: cancelled_count as i64,
        total_count: total_count as i64,
    };

    let response = GetStatsResponse {
        response: Some(get_stats_response::Response::Success(stats)),
    };

    Ok(Json(response))
}

#[axum::debug_handler]
pub async fn cancel_tasks(
    State(state): State<AppState>,
    Json(request): Json<CancelTasksRequest>,
) -> Result<Json<CancelTasksResponse>, ApiError> {
    let mut task_names = Vec::new();
    for selector in request.selectors {
        if let Some(Selector::Name(name)) = selector.selector {
            task_names.push(name);
        }
    }

    let mut multiqueue = state.multiqueue.lock().await;
    let count = multiqueue.cancel_tasks(&task_names).await?;

    let response = CancelTasksResponse {
        response: Some(cancel_tasks_response::Response::Count(count as i64)),
    };

    Ok(Json(response))
}
