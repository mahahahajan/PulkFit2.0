-- ============================================================
--  PulkFit 2.0 — Supabase Schema
--  Run this once in the Supabase SQL Editor (Dashboard → SQL)
-- ============================================================

-- Table 1: Daily biometric + nutrition entries
CREATE TABLE IF NOT EXISTS daily_metrics (
    date        DATE    PRIMARY KEY,
    weight_lbs  REAL,
    sleep_hours REAL,
    step_count  INTEGER,
    calories    INTEGER,   -- nullable: not always logged
    protein_g   INTEGER    -- nullable: not always logged
);

-- Table 2: Best set per movement per day (upsertable)
CREATE TABLE IF NOT EXISTS lifting_log (
    id          UUID    PRIMARY KEY DEFAULT gen_random_uuid(),
    date        DATE    NOT NULL,
    movement    VARCHAR NOT NULL,
    weight_lbs  REAL,
    sets        INTEGER,
    reps        INTEGER,
    est_1rm     REAL,       -- Brzycki formula, calculated on ingestion
    CONSTRAINT lifting_log_date_movement_key UNIQUE (date, movement)
);

-- Table 3: OAuth token store for background API sync
CREATE TABLE IF NOT EXISTS auth_tokens (
    service       VARCHAR   PRIMARY KEY,   -- 'fitbit', 'strava', etc.
    access_token  TEXT      NOT NULL,
    refresh_token TEXT      NOT NULL,
    expires_at    TIMESTAMP NOT NULL
);

-- ── Indexes ────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_daily_metrics_date     ON daily_metrics (date DESC);
CREATE INDEX IF NOT EXISTS idx_lifting_log_date       ON lifting_log (date DESC);
CREATE INDEX IF NOT EXISTS idx_lifting_log_movement   ON lifting_log (movement, date DESC);

-- ── 7-Day Rolling Average View ─────────────────────────────
-- Query this view instead of daily_metrics for the dashboard.
-- All averages use a 6-preceding-row window (current day inclusive).
CREATE OR REPLACE VIEW rolling_averages AS
SELECT
    date,
    weight_lbs,
    sleep_hours,
    step_count,
    calories,
    protein_g,
    ROUND(CAST(AVG(weight_lbs)  OVER w AS NUMERIC), 2) AS avg_weight_7d,
    ROUND(CAST(AVG(sleep_hours) OVER w AS NUMERIC), 2) AS avg_sleep_7d,
    ROUND(CAST(AVG(step_count)  OVER w AS NUMERIC), 0) AS avg_steps_7d,
    ROUND(CAST(AVG(calories)    OVER w AS NUMERIC), 0) AS avg_calories_7d,
    ROUND(CAST(AVG(protein_g)   OVER w AS NUMERIC), 0) AS avg_protein_7d
FROM daily_metrics
WINDOW w AS (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
ORDER BY date;

-- ── Verify ─────────────────────────────────────────────────
-- After running, confirm with:
--   SELECT table_name FROM information_schema.tables
--   WHERE table_schema = 'public';
