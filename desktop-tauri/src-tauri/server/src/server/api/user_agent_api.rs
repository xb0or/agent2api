//! GET/PUT /api/user-agent —— 全局默认出站 User-Agent。
//!
//! ── 这一格管什么 ────────────────────────────────────────────
//! 网关所有 `reqwest` 出站客户端（账号代理 / 直连 / 系统代理三条出口）在构造时
//! 统一带上的默认 UA，出厂值 `undici`（与 Node 版一致，见
//! `core::egress` 文件头的实测记录）。用户想伪装成别的客户端形态时在这里改。
//!
//! 优先级：**请求级 `User-Agent` 头 > 本设置 > 出厂值**。reqwest 的客户端级
//! `user_agent` 只在 HeaderMap 空缺时插入，所以各适配器自己设置的 UA
//! （Cline 伪装头、OpenCode 会话头等）不受影响。
//!
//! ── 生效时机 ────────────────────────────────────────────────
//! 保存后**下一次建客户端**即生效：缓存键里含 UA（见 `core::egress::cache_key`），
//! 旧 Client 只服务在途请求，随后被 LRU 淘汰。不需要重启。
//!
//! ── 校验 ────────────────────────────────────────────────────
//! 只拒绝会破坏 HTTP 头的东西：换行 / 其他控制字符、超长。空串 = 清空覆盖、
//! 回落出厂值。不做「像不像浏览器」这类判断 —— 用户要的就是自选。

use axum::body::Bytes;
use axum::extract::State;
use axum::response::Response;
use serde_json::{json, Value};

use crate::server::config::{
    self, DEFAULT_EGRESS_USER_AGENT, KEY_EGRESS_USER_AGENT, MAX_EGRESS_USER_AGENT_CHARS,
};
use crate::server::errors;
use crate::server::http::{ok_json, parse_body};
use crate::server::logging;
use crate::server::ServerState;

/// GET /api/user-agent —— 当前生效值与出厂值。
///
/// `custom` 让界面不必自己比较字符串（出厂值将来换字面量时前端不会跟着错）。
pub async fn get_user_agent(State(_state): State<ServerState>) -> Response {
    ok_json(user_agent_json())
}

/// PUT /api/user-agent —— body `{"userAgent": "..."}`（空串 = 回落出厂值）。
///
/// 返回生效后的全量值（与 /api/timeouts 同一契约：前端直接用响应刷新界面）。
pub async fn put_user_agent(State(_state): State<ServerState>, body: Bytes) -> Response {
    let payload = match parse_body(&body) {
        Ok(value) => value,
        Err(error) => return errors::management_error(400, error.message),
    };
    let Some(object) = payload.as_object() else {
        return errors::management_error(400, "请求体必须是 JSON 对象");
    };
    let Some(raw) = object.get(KEY_EGRESS_USER_AGENT) else {
        return errors::management_error(400, format!("缺少 {KEY_EGRESS_USER_AGENT}"));
    };
    let Some(text) = raw.as_str() else {
        return errors::management_error(400, format!("{KEY_EGRESS_USER_AGENT} 必须是字符串"));
    };
    let trimmed = text.trim();
    if let Err(message) = validated(trimmed) {
        return errors::management_error(400, message);
    }
    if !config::set_egress_user_agent(trimmed) {
        // 与超时设置同一处理：内存里的 raw 已更新（本次运行生效），但要如实告知
        // 「重启后会回去」，否则会被当成玄学
        logging::log("[Config]", "⚠️  出站 User-Agent 写入失败，本次运行内仍生效");
    }
    logging::log(
        "[Config]",
        &format!(
            "出站 User-Agent 设为 {}",
            if trimmed.is_empty() {
                format!("出厂值 {DEFAULT_EGRESS_USER_AGENT}")
            } else {
                trimmed.to_string()
            }
        ),
    );
    ok_json(user_agent_json())
}

fn user_agent_json() -> Value {
    let user_agent = config::egress_user_agent();
    json!({
        "userAgent": user_agent,
        "defaultUserAgent": DEFAULT_EGRESS_USER_AGENT,
        "custom": user_agent != DEFAULT_EGRESS_USER_AGENT,
    })
}

/// 校验：空串合法；其余不得含控制字符（换行会拆坏请求头），长度按字符数限制。
fn validated(text: &str) -> Result<(), String> {
    if text.chars().count() > MAX_EGRESS_USER_AGENT_CHARS {
        return Err(format!(
            "User-Agent 最多 {MAX_EGRESS_USER_AGENT_CHARS} 个字符"
        ));
    }
    if text.chars().any(char::is_control) {
        return Err("User-Agent 不能包含换行或其他控制字符".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_value_clears_the_override() {
        assert!(validated("").is_ok());
    }

    #[test]
    fn a_normal_user_agent_is_accepted() {
        assert!(validated("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124.0").is_ok());
        assert!(validated("undici").is_ok());
    }

    #[test]
    fn control_characters_are_rejected() {
        assert!(validated("undici\r\nX-Injected: 1").is_err());
        assert!(validated("undici\tx").is_err());
    }

    #[test]
    fn an_over_long_user_agent_is_rejected() {
        assert!(validated(&"a".repeat(MAX_EGRESS_USER_AGENT_CHARS + 1)).is_err());
    }
}
