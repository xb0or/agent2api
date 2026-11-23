-- ── requests 补上下游 request id（schema v34）──────────────────────
-- 值形如 `x-request-id: xxx` 或 `cf-ray: yyy`，保留来源头名。
ALTER TABLE requests ADD COLUMN downstream_request_id TEXT NOT NULL DEFAULT '';
ALTER TABLE requests ADD COLUMN upstream_request_id TEXT NOT NULL DEFAULT '';
