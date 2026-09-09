use polars::prelude::{DataType, Schema};

/// The fields every line logs around its stream's own row, whatever the
/// stream: the gateway's json formatter's stamps and the stream's name.
const ENVELOPE: &[(&str, DataType)] = &[
    ("timestamp", DataType::String),
    ("level", DataType::String),
    ("logger", DataType::String),
    ("pid", DataType::Int64),
    ("lineno", DataType::Int64),
    ("access_id", DataType::String),
    ("message", DataType::String),
    ("stream", DataType::String),
];

/// The schema of each stream's rows, mirrored from the structured logs'
/// pydantic models of `first_common` — `schema/structured_logs.py` and
/// `schema/auth.py` — and the fields their emitters add around them
/// (`user.name`, `user.id`); the datetimes stay the iso strings the lines
/// log them as. `None` for the streams that hold no structured rows.
pub fn schema_of(stream: &str) -> Option<Schema> {
    use DataType::*;

    let columns = match stream {
        // AccessLog, its emit adding the user's id
        "access_log" => vec![
            ("id", String),
            ("timestamp_request", String),
            ("api_route", String),
            ("origin_ip", String),
            ("timestamp_response", String),
            ("status_code", Int64),
            ("error", String),
            ("authorized_groups", String),
            ("user.id", String),
        ],
        // BatchLog
        "batch_log" => vec![
            ("id", String),
            ("access_log_id", String),
            ("user_id", String),
            ("input_file", String),
            ("output_folder_path", String),
            ("cluster", String),
            ("framework", String),
            ("model", String),
            ("globus_batch_uuid", String),
            ("task_ids", String),
            ("result", String),
            ("status", String),
            ("in_progress_at", String),
            ("completed_at", String),
            ("failed_at", String),
        ],
        // BatchLog.emit_metrics
        "batch_metrics" => vec![
            ("batch_id", String),
            ("cluster", String),
            ("framework", String),
            ("model", String),
            ("status", String),
            ("total_tokens", Int64),
            ("num_responses", Int64),
            ("response_time_sec", Float64),
            ("throughput_tokens_per_sec", Float64),
            ("completed_at", String),
        ],
        // RequestLog
        "request_log" => vec![
            ("id", String),
            ("access_log_id", String),
            ("user_id", String),
            ("cluster", String),
            ("framework", String),
            ("model", String),
            ("openai_endpoint", String),
            ("prompt", String),
            ("timestamp_compute_request", String),
            ("status_code", Int64),
            ("timestamp_compute_response", String),
            ("result", String),
            ("task_uuid", String),
        ],
        // RequestMetrics, its computed fields included
        "request_metrics" => vec![
            ("request_id", String),
            ("cluster", String),
            ("framework", String),
            ("model", String),
            ("timestamp_compute_request", String),
            ("timestamp_compute_response", String),
            ("status_code", Int64),
            ("prompt_tokens", Int64),
            ("completion_tokens", Int64),
            ("total_tokens", Int64),
            ("response_time_sec", Float64),
            ("throughput_tokens_per_sec", Float64),
        ],
        // UserAuthEvent, its emit adding the user's name
        "user" => vec![
            ("id", String),
            ("username", String),
            ("user_group_uuids", List(Box::new(String))),
            ("authorized_group_uuids", String),
            ("idp_id", String),
            ("idp_name", String),
            ("auth_service", String),
            ("user.name", String),
        ],
        _ => return None,
    };

    // the envelope around the stream's own columns
    let mut schema = Schema::default();
    for (name, dtype) in ENVELOPE.iter().cloned().chain(columns) {
        schema.insert(name.into(), dtype);
    }

    Some(schema)
}
