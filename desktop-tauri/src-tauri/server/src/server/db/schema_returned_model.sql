-- ── requests 补「上游实际返回模型」列（schema v33）────────────────
-- 诊断列：记录上游响应体里的 model 字段，与「我们发出去的上游模型」分开。
-- 空串 = 未采到（旧行 / 上游没给 / 响应前就失败）。
ALTER TABLE requests ADD COLUMN returned_model TEXT NOT NULL DEFAULT '';
