//! 下游流式连接心跳保活：`GET/PUT /api/stream-keepalive`。
//!
//! SSE 在静默时补 `: keepalive` 注释行，WebSocket 补协议层 Ping；两者都只
//! 制造链路流量，不进入客户端业务事件。开关与间隔下一次新建流 / 连接生效。

use axum::body::Bytes;
use axum::extract::State;
use axum::response::Response;
use serde_json::{json, Value};

use crate::server::config::{
    self, DEFAULT_STREAM_KEEPALIVE_SECONDS, MAX_STREAM_KEEPALIVE_SECONDS,
    MIN_STREAM_KEEPALIVE_SECONDS,
};
use crate::server::errors;
use crate::server::http::{ok_json, parse_body};
use crate::server::logging;
use crate::server::ServerState;

pub async fn get_stream_keepalive(State(_state): State<ServerState>) -> Response {
    ok_json(state_json())
}

pub async fn put_stream_keepalive(State(_state): State<ServerState>, body: Bytes) -> Response {
    let payload = match parse_body(&body) {
        Ok(value) => value,
        Err(error) => return errors::management_error(400, error.message),
    };
    let Some(object) = payload.as_object() else {
        return errors::management_error(400, "请求体必须是 JSON 对象");
    };
    let enabled = match object.get("enabled") {
        None => None,
        Some(value) => match value.as_bool() {
            Some(value) => Some(value),
            None => return errors::management_error(400, "enabled 必须是布尔值"),
        },
    };
    let seconds = match object.get("seconds") {
        None => None,
        Some(value) => match value.as_i64() {
            Some(value) if (MIN_STREAM_KEEPALIVE_SECONDS..=MAX_STREAM_KEEPALIVE_SECONDS).contains(&value) => {
                Some(value)
            }
            _ => {
                return errors::management_error(
                    400,
                    format!("seconds 必须是 {MIN_STREAM_KEEPALIVE_SECONDS}~{MAX_STREAM_KEEPALIVE_SECONDS} 的整数"),
                )
            }
        },
    };
    if enabled.is_none() && seconds.is_none() {
        return errors::management_error(400, "缺少 enabled 或 seconds");
    }
    if let Some(enabled) = enabled {
        if !config::set_stream_keepalive_enabled(enabled) {
            logging::log("[Config]", "⚠️  心跳保活开关写入失败，本次运行内仍生效");
        }
        logging::log("[Config]", &format!("下游心跳保活{}", if enabled { "已开启" } else { "已关闭" }));
    }
    if let Some(seconds) = seconds {
        if !config::set_stream_keepalive_seconds(seconds) {
            logging::log("[Config]", "⚠️  心跳间隔写入失败，本次运行内仍生效");
        }
        logging::log("[Config]", &format!("下游心跳间隔设为 {seconds} 秒"));
    }
    ok_json(state_json())
}

fn state_json() -> Value {
    json!({
        "enabled": config::stream_keepalive_enabled(),
        "seconds": config::stream_keepalive_seconds(),
        "defaultSeconds": DEFAULT_STREAM_KEEPALIVE_SECONDS,
        "minSeconds": MIN_STREAM_KEEPALIVE_SECONDS,
        "maxSeconds": MAX_STREAM_KEEPALIVE_SECONDS,
    })
}
