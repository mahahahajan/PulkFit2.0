"""
PulkFit 2.0 — app.py
====================
Phases 2 & 3: Core dashboard + ingestion workflows.

Deploy to Streamlit Community Cloud:
  1. Push this file (and requirements.txt) to a public GitHub repo.
  2. Go to share.streamlit.io → New app → select repo.
  3. Add all .env variables as Streamlit Secrets
     (Dashboard → Settings → Secrets — same key names as .env).

Local dev:
  streamlit run app.py
"""

from __future__ import annotations

import io
import os
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
from supabase import Client, create_client

# ══════════════════════════════════════════════════════════════
#  Configuration
# ══════════════════════════════════════════════════════════════

def _env(key: str, default: str = "") -> str:
    """Read from Streamlit Secrets first, then os.environ."""
    try:
        return st.secrets[key]
    except (KeyError, FileNotFoundError):
        return os.environ.get(key, default)


SUPABASE_URL              = _env("SUPABASE_URL")
SUPABASE_PUBLISHABLE_KEY  = _env("SUPABASE_PUBLISHABLE_KEY")
FITBIT_CLIENT_ID     = _env("FITBIT_CLIENT_ID")
FITBIT_CLIENT_SECRET = _env("FITBIT_CLIENT_SECRET")
# Streamlit Cloud public URL — used as the Fitbit OAuth redirect target.
# Set this to your deployed app URL, e.g. https://yourapp.streamlit.app/
FITBIT_REDIRECT_URI  = _env("FITBIT_REDIRECT_URI", "http://localhost:8501/")

# ── Macrocycle targets ─────────────────────────────────────
TARGETS = {
    "weight_lbs":   float(_env("TARGET_WEIGHT_LBS",   "145.0")),
    "sleep_hours":  float(_env("TARGET_SLEEP_HOURS",  "7.0")),
    "step_count":   int(_env("TARGET_STEP_COUNT",      "12000")),
    "calories":     int(_env("TARGET_CALORIES",        "2600")),
    "protein_g":    int(_env("TARGET_PROTEIN_G",       "150")),
    "squat_1rm":    float(_env("TARGET_SQUAT_1RM",     "315")),
    "bench_1rm":    float(_env("TARGET_BENCH_1RM",     "225")),
    "deadlift_1rm": float(_env("TARGET_DEADLIFT_1RM",  "405")),
}

# Per-metric daily thresholds for the hit-rate scorecard
# Weight uses the target as a ceiling (cutting); everything else is a floor.
HIT_THRESHOLDS = {
    "weight_lbs":  ("lte", TARGETS["weight_lbs"]),   # ≤ target = hit
    "sleep_hours": ("gte", TARGETS["sleep_hours"]),   # ≥ target = hit
    "step_count":  ("gte", TARGETS["step_count"]),
    "calories":    ("gte", TARGETS["calories"]),
    "protein_g":   ("gte", TARGETS["protein_g"]),
}

STRENGTH_MOVEMENTS = {
    "squat":     ["squat", "back squat", "barbell squat", "low bar squat"],
    "bench":     ["bench", "bench press", "barbell bench", "flat bench"],
    "deadlift":  ["deadlift", "conventional deadlift", "sumo deadlift"],
}


# ══════════════════════════════════════════════════════════════
#  Supabase Client
# ══════════════════════════════════════════════════════════════

@st.cache_resource
def get_supabase() -> Client:
    url = SUPABASE_URL.strip().strip('"').strip("'")
    key = SUPABASE_PUBLISHABLE_KEY.strip().strip('"').strip("'")
    if not url or not key:
        st.error(
            f"Missing Supabase credentials.\n\n"
            f"SUPABASE_URL: {'set (' + str(len(url)) + ' chars)' if url else '**MISSING**'}\n\n"
            f"SUPABASE_PUBLISHABLE_KEY: {'set (' + str(len(key)) + ' chars)' if key else '**MISSING**'}"
        )
        st.stop()
    return create_client(url, key)


# ══════════════════════════════════════════════════════════════
#  Fitbit Token Management
# ══════════════════════════════════════════════════════════════

def load_tokens() -> dict | None:
    """Return stored Fitbit token row from Supabase, or None."""
    db = get_supabase()
    res = db.table("auth_tokens").select("*").eq("service", "fitbit").maybe_single().execute()
    return res.data if res.data else None


def refresh_fitbit_token(refresh_token: str) -> dict | None:
    """
    Try to exchange a refresh token for a new access token.
    Returns the new token dict, or None on failure.
    """
    try:
        resp = requests.post(
            "https://api.fitbit.com/oauth2/token",
            data={"grant_type": "refresh_token", "refresh_token": refresh_token},
            auth=(FITBIT_CLIENT_ID, FITBIT_CLIENT_SECRET),
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def save_tokens(tokens: dict):
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=tokens.get("expires_in", 28800))
    get_supabase().table("auth_tokens").upsert(
        {
            "service":       "fitbit",
            "access_token":  tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "expires_at":    expires_at.isoformat(),
        },
        on_conflict="service",
    ).execute()


def get_valid_access_token() -> str | None:
    """
    Return a valid Fitbit access token, refreshing automatically if needed.
    Returns None if no tokens are stored or the refresh token has expired.
    """
    row = load_tokens()
    if not row:
        return None

    expires_at = datetime.fromisoformat(row["expires_at"])
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    # Refresh if within 5 minutes of expiry
    if datetime.now(timezone.utc) >= expires_at - timedelta(minutes=5):
        new_tokens = refresh_fitbit_token(row["refresh_token"])
        if not new_tokens:
            return None  # refresh token expired — user must re-auth
        save_tokens(new_tokens)
        return new_tokens["access_token"]

    return row["access_token"]


def fitbit_auth_url() -> str:
    import urllib.parse
    return (
        "https://www.fitbit.com/oauth2/authorize"
        f"?response_type=code"
        f"&client_id={FITBIT_CLIENT_ID}"
        f"&redirect_uri={urllib.parse.quote(FITBIT_REDIRECT_URI)}"
        f"&scope={urllib.parse.quote('weight sleep activity')}"
        f"&expires_in=604800"
    )


def exchange_fitbit_code(code: str) -> dict | None:
    """Exchange an OAuth authorization code for tokens."""
    try:
        resp = requests.post(
            "https://api.fitbit.com/oauth2/token",
            data={
                "grant_type":  "authorization_code",
                "code":         code,
                "redirect_uri": FITBIT_REDIRECT_URI,
            },
            auth=(FITBIT_CLIENT_ID, FITBIT_CLIENT_SECRET),
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════
#  Fitbit Data Pull
# ══════════════════════════════════════════════════════════════

def fetch_fitbit_week(access_token: str) -> pd.DataFrame:
    """Pull the past 7 days of steps, sleep, and weight from Fitbit."""
    end_dt   = date.today() - timedelta(days=1)
    start_dt = end_dt - timedelta(days=6)
    s, e     = start_dt.isoformat(), end_dt.isoformat()

    records: dict[str, dict] = {}

    def _get(endpoint: str) -> dict:
        r = requests.get(
            f"https://api.fitbit.com{endpoint}",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

    for entry in _get(f"/1/user/-/body/weight/date/{s}/{e}.json").get("body-weight", []):
        records.setdefault(entry["dateTime"], {})["weight_lbs"] = round(float(entry["value"]) * 2.20462, 1)

    for entry in _get(f"/1.2/user/-/sleep/date/{s}/{e}.json").get("sleep", []):
        d     = entry["dateOfSleep"]
        hours = entry.get("minutesAsleep", 0) / 60
        prev  = records.setdefault(d, {}).get("sleep_hours", 0.0)
        records[d]["sleep_hours"] = round(prev + hours, 2)

    for entry in _get(f"/1/user/-/activities/steps/date/{s}/{e}.json").get("activities-steps", []):
        records.setdefault(entry["dateTime"], {})["step_count"] = int(entry["value"])

    for entry in _get(f"/1/user/-/activities/calories/date/{s}/{e}.json").get("activities-calories", []):
        records.setdefault(entry["dateTime"], {})["calories"] = int(float(entry["value"]))

    rows = [{"date": d, **v} for d, v in records.items()]
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ══════════════════════════════════════════════════════════════
#  Hevy CSV Processing (Phase 3)
# ══════════════════════════════════════════════════════════════

_HEVY_COL_MAP = {
    "exercise_title":     "movement",
    "exercise_name":      "movement",
    "start_time":         "date_raw",
    "workout_start_time": "date_raw",
    "date":               "date_raw",
    "weight_(lbs)":       "weight_lbs",
    "weight_lbs":         "weight_lbs",
    "weight":             "weight_lbs",
    "reps":               "reps",
}


def brzycki_1rm(weight: float, reps: int) -> float:
    if reps < 1 or reps >= 37 or weight <= 0:
        return round(weight, 1)
    return round(weight * (36 / (37 - reps)), 1)


def process_hevy_df(raw: pd.DataFrame) -> pd.DataFrame:
    raw.columns = [c.strip().lower().replace(" ", "_") for c in raw.columns]
    raw = raw.rename(columns={k: v for k, v in _HEVY_COL_MAP.items() if k in raw.columns})

    raw["date"]       = pd.to_datetime(raw["date_raw"], utc=True, errors="coerce").dt.date.astype(str)
    raw["weight_lbs"] = pd.to_numeric(raw.get("weight_lbs", 0), errors="coerce").fillna(0.0)
    raw["reps"]       = pd.to_numeric(raw.get("reps",       0), errors="coerce").fillna(0).astype(int)
    raw["movement"]   = raw["movement"].str.strip()
    raw["est_1rm"]    = raw.apply(lambda r: brzycki_1rm(r["weight_lbs"], r["reps"]), axis=1)

    best_idx = raw.groupby(["date", "movement"])["est_1rm"].idxmax()
    best     = raw.loc[best_idx, ["date", "movement", "weight_lbs", "reps", "est_1rm"]].copy()
    counts   = raw.groupby(["date", "movement"]).size().reset_index(name="sets")
    return best.merge(counts, on=["date", "movement"], how="left")


# ══════════════════════════════════════════════════════════════
#  Data Loading
# ══════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def load_rolling_averages() -> pd.DataFrame:
    db  = get_supabase()
    res = db.table("rolling_averages").select("*").order("date", desc=True).limit(90).execute()
    df  = pd.DataFrame(res.data or [])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def prev_rolling_avgs(df: pd.DataFrame) -> dict:
    """Return rolling averages from 7 days ago for week-over-week comparison."""
    if len(df) < 8:
        return {}
    row = df.sort_values("date", ascending=False).iloc[7]
    return row.to_dict()


@st.cache_data(ttl=300)
def load_this_week() -> pd.DataFrame:
    """Return raw daily_metrics rows for the current Mon–Sun week."""
    today    = date.today()
    monday   = today - timedelta(days=today.weekday())
    sunday   = monday + timedelta(days=6)
    db  = get_supabase()
    res = db.table("daily_metrics").select("*")         .gte("date", monday.isoformat())         .lte("date", sunday.isoformat())         .order("date").execute()
    df = pd.DataFrame(res.data or [])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


@st.cache_data(ttl=300)
def load_lifting_log() -> pd.DataFrame:
    db  = get_supabase()
    res = db.table("lifting_log").select("*").order("date", desc=True).limit(500).execute()
    df  = pd.DataFrame(res.data or [])
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
    return df


def current_rolling_avgs(df: pd.DataFrame) -> dict:
    """Return the most recent row of rolling averages as a plain dict."""
    if df.empty:
        return {}
    row = df.sort_values("date", ascending=False).iloc[0]
    return row.to_dict()


def best_1rm(lift_df: pd.DataFrame, movement_aliases: list[str]) -> float | None:
    """Return the all-time best estimated 1RM for a movement (by aliases)."""
    if lift_df.empty:
        return None
    mask = lift_df["movement"].str.lower().apply(
        lambda m: any(alias in m for alias in movement_aliases)
    )
    sub = lift_df[mask]
    return float(sub["est_1rm"].max()) if not sub.empty else None


# ══════════════════════════════════════════════════════════════
#  Marine PFT Scoring (Male, Age 26–30)
#  Max 300 pts total: 100 pull-ups + 100 plank + 100 run
# ══════════════════════════════════════════════════════════════

# Pull-up points table — Male, 26–30
# reps → points
_PULLUP_POINTS = {
    23: 100, 22: 97, 21: 93, 20: 90, 19: 86, 18: 83,
    17: 79,  16: 76, 15: 72, 14: 69, 13: 65, 12: 62,
    11: 59,  10: 55,  9: 52,  8: 48,  7: 45,  6: 41,
     5: 38,   4: 34,  3: 31,  2: 20,  1: 10,  0:  0,
}

# Plank points table — Male, 26–30
# duration in seconds → points (sampled at key breakpoints)
_PLANK_POINTS = [
    (225, 100), (220, 98), (215, 96), (210, 94), (205, 92),
    (200, 90),  (195, 88), (190, 86), (185, 84), (180, 82),
    (175, 80),  (170, 78), (165, 76), (160, 74), (155, 72),
    (150, 70),  (145, 68), (140, 66), (135, 64), (130, 62),
    (125, 60),  (120, 58), (115, 56), (110, 54), (105, 52),
    (100, 50),  ( 95, 48), ( 90, 46), ( 85, 44), ( 80, 42),
    ( 75, 41),  ( 70, 40), ( 63, 40),             # 1:03 minimum = 40 pts
]


def score_pullups(reps: int) -> int:
    """Return PFT points for pull-up reps (Male 26–30). Caps at 23 reps."""
    reps = max(0, min(int(reps), 23))
    return _PULLUP_POINTS.get(reps, 0)


def score_plank(seconds: int) -> int:
    """Return PFT points for plank duration in seconds (Male 26–30)."""
    for threshold, pts in _PLANK_POINTS:
        if seconds >= threshold:
            return pts
    return 0  # below 1:03 minimum


def pft_grade(total: int) -> str:
    if total >= 270: return "1st Class"
    if total >= 235: return "2nd Class"
    if total >= 200: return "3rd Class"
    return "Fail"


# ── Movement alias lookup ──────────────────────────────────────────────────

_ALIASES = {
    "lat_pulldown":    ["lat pulldown", "lat pull down", "lat pull-down", "pulldown"],
    "assisted_pullup": ["assisted pull up", "assisted chin"],
    "pullup":          ["pull up (bodyweight)"],   # exact Hevy name only
    "pullup_negative": ["negatiev pull up", "negative pull up", "pull up negative"],
    "bench":           ["bench press", "barbell bench", "flat bench", "bench"],
    "dip":             ["dip"],
    "pushup":          ["push up", "push-up", "pushup"],
    "plank":           ["plank"],
}


def _match(movement: str, aliases: list[str]) -> bool:
    """
    Match a movement name against a list of aliases.
    Single-item alias lists use exact match; multi-item lists use substring match.
    This prevents broad aliases like "pull up" from catching "assisted pull up".
    """
    m = movement.lower().strip()
    if len(aliases) == 1:
        return m == aliases[0].lower()
    return any(a.lower() in m for a in aliases)


def _best_1rm(lift_df: pd.DataFrame, key: str, days: int = 90) -> float | None:
    """Return best est_1rm for a movement group within the past N days."""
    if lift_df.empty:
        return None
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    aliases = _ALIASES[key]
    mask = (
        lift_df["movement"].apply(lambda m: _match(m, aliases))
        & (lift_df["date"] >= cutoff)
    )
    sub = lift_df[mask]
    return float(sub["est_1rm"].max()) if not sub.empty else None


def _best_reps(lift_df: pd.DataFrame, key: str, days: int = 90) -> int | None:
    """Return max reps logged for a movement group within the past N days."""
    if lift_df.empty:
        return None
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    aliases = _ALIASES[key]
    mask = (
        lift_df["movement"].apply(lambda m: _match(m, aliases))
        & (lift_df["date"] >= cutoff)
    )
    sub = lift_df[mask]
    return int(sub["reps"].max()) if not sub.empty else None


def _assisted_effective_1rm(lift_df: pd.DataFrame, bodyweight: float, days: int = 90) -> float | None:
    """
    For assisted pull-up machine: weight logged = assistance weight (subtracted from bw).
    Effective load = bodyweight - assistance. Returns best effective 1RM.
    """
    if lift_df.empty:
        return None
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=days)
    aliases = _ALIASES["assisted_pullup"]
    mask = (
        lift_df["movement"].apply(lambda m: _match(m, aliases))
        & (lift_df["date"] >= cutoff)
        & (lift_df["weight_lbs"] > 0)
    )
    sub = lift_df[mask].copy()
    if sub.empty:
        return None
    sub["effective_lbs"] = bodyweight - sub["weight_lbs"]
    sub = sub[sub["effective_lbs"] > 0]
    if sub.empty:
        return None
    # Recalculate 1RM on effective weight
    sub["eff_1rm"] = sub.apply(
        lambda r: brzycki_1rm(r["effective_lbs"], int(r["reps"])), axis=1
    )
    return float(sub["eff_1rm"].max())


def estimate_pullup_reps(lift_df: pd.DataFrame, bodyweight: float) -> dict:
    """
    Estimate max pull-up reps from multiple strength signals.
    Returns a dict with per-signal estimates and the final synthesized value.

    Method:
      - Lat pulldown 1RM / bodyweight ratio → reps via linear model
        (1RM = 100% bw ≈ 1 rep; each additional 3.5% ≈ +1 rep)
      - Assisted pull-up effective 1RM / bodyweight → same model
      - Actual pull-up reps → hard floor
      - Negative pull-ups → signals capability, adds +1 floor if no reps logged
      Final = max(all signals), never below actual reps logged.
    """
    signals = {}

    lat_1rm = _best_1rm(lift_df, "lat_pulldown", days=90)
    if lat_1rm is not None and bodyweight > 0:
        ratio = lat_1rm / bodyweight
        est   = max(0, int((ratio - 1.0) / 0.035)) if ratio >= 1.0 else 0
        signals["lat_pulldown"] = {"est": est, "detail": f"1RM {lat_1rm:.0f} lbs = {ratio:.0%} bw"}

    asst_1rm = _assisted_effective_1rm(lift_df, bodyweight, days=90)
    if asst_1rm is not None and bodyweight > 0:
        ratio = asst_1rm / bodyweight
        est   = max(0, int((ratio - 1.0) / 0.035)) if ratio >= 1.0 else 0
        signals["assisted_pullup"] = {"est": est, "detail": f"effective 1RM {asst_1rm:.0f} lbs = {ratio:.0%} bw"}

    actual_reps = _best_reps(lift_df, "pullup", days=90)
    if actual_reps is not None:
        signals["actual_pullup"] = {"est": actual_reps, "detail": f"{actual_reps} reps logged directly"}

    has_negatives = _best_reps(lift_df, "pullup_negative", days=90) is not None
    if has_negatives and not signals:
        signals["negatives"] = {"est": 1, "detail": "negatives logged — near pull-up threshold"}

    final = max((v["est"] for v in signals.values()), default=0)
    return {"reps": final, "signals": signals}


def estimate_pushup_reps(lift_df: pd.DataFrame, bodyweight: float) -> dict:
    """
    Estimate max push-up reps from bench press and dip strength.

    Method:
      - Push-up loads ~64% of bodyweight (research-backed constant)
      - Bench 1RM / (0.64 × bw) = strength ratio
        ratio 1.0 → ~20 reps; each +0.05 ratio → ~+5 reps (linear approximation)
      - Bodyweight dip 1RM ≈ pressing bodyweight → cross-validates bench estimate
      - Actual push-up reps → hard floor
    """
    signals = {}
    pushup_load = 0.64 * bodyweight if bodyweight > 0 else None

    bench_1rm = _best_1rm(lift_df, "bench", days=90)
    if bench_1rm is not None and pushup_load:
        ratio = bench_1rm / pushup_load
        est   = max(0, int(20 + (ratio - 1.0) / 0.05 * 5))
        signals["bench"] = {"est": est, "detail": f"bench 1RM {bench_1rm:.0f} lbs, push-up load {pushup_load:.0f} lbs ({ratio:.1f}×)"}

    dip_1rm = _best_1rm(lift_df, "dip", days=90)
    if dip_1rm is not None and pushup_load:
        # Dips load ~bodyweight; use same ratio model as bench
        ratio = dip_1rm / pushup_load
        est   = max(0, int(20 + (ratio - 1.0) / 0.05 * 5))
        signals["dips"] = {"est": est, "detail": f"dip 1RM {dip_1rm:.0f} lbs vs push-up load {pushup_load:.0f} lbs"}

    actual_reps = _best_reps(lift_df, "pushup", days=90)
    if actual_reps is not None:
        signals["actual_pushup"] = {"est": actual_reps, "detail": f"{actual_reps} reps logged directly"}

    final = max((v["est"] for v in signals.values()), default=0)
    return {"reps": final, "signals": signals}


def marine_readiness_score(lift_df: pd.DataFrame, bodyweight: float | None) -> dict:
    """
    Compute Marine PFT score using inferred bodyweight-exercise capacity.
    Pull-up and push-up reps are estimated from weighted lift progressions.
    Plank uses reps column as seconds (log 90 reps = 90 seconds).
    Run is stubbed pending Phase 4 Strava integration.
    """
    bw = bodyweight or 0.0

    pullup_est = estimate_pullup_reps(lift_df, bw)
    pushup_est = estimate_pushup_reps(lift_df, bw)

    pullup_reps = pullup_est["reps"]
    pullup_pts  = score_pullups(pullup_reps)

    plank_secs = _best_reps(lift_df, "plank", days=90)  # reps column = seconds
    plank_pts  = score_plank(plank_secs) if plank_secs is not None else None

    total = pullup_pts + (plank_pts or 0)

    return {
        "pullup_reps":    pullup_reps,
        "pullup_pts":     pullup_pts,
        "pullup_signals": pullup_est["signals"],
        "pushup_reps":    pushup_est["reps"],
        "pushup_signals": pushup_est["signals"],
        "plank_secs":     plank_secs,
        "plank_pts":      plank_pts,
        "run_pts":        None,
        "total":          total,
        "total_max":      200,
        "grade":          pft_grade(total) if (pullup_pts and plank_pts) else "Incomplete",
    }


# ══════════════════════════════════════════════════════════════
#  UI Components
# ══════════════════════════════════════════════════════════════

def weekly_hit_rate(df: pd.DataFrame, col: str, threshold: float,
                    direction: str = "gte") -> tuple[int, int, list]:
    """
    For the current ISO week (Mon–today), count how many days hit the threshold.
    Returns (hits, days_with_data, list_of_bools_per_day).
    direction: "gte" = value >= threshold (sleep, steps), "lte" = value <= threshold (weight).
    """
    import zoneinfo
    tz         = zoneinfo.ZoneInfo("America/New_York")
    today      = pd.Timestamp.now(tz=tz).normalize().tz_localize(None)
    week_start = today - pd.Timedelta(days=today.dayofweek)  # Monday
    week_df = df[(df["date"] >= week_start) & (df["date"] <= today)].copy()
    week_df = week_df.dropna(subset=[col])
    if week_df.empty:
        return 0, 0, []
    if direction == "gte":
        hits_series = week_df[col] >= threshold
    else:
        hits_series = week_df[col] <= threshold
    hits = int(hits_series.sum())
    return hits, len(week_df), hits_series.tolist()


def scorecard_tile(label: str, hits: int, total: int, unit: str,
                   avg_val, threshold: float, lower_is_better: bool = False):
    """
    Render a hit-rate tile.
    Green = 5+/7 days on target, Amber = 3-4, Red = 0-2, Grey = no data.
    """
    if total == 0:
        st.metric(f"⚪ {label}", "no data", help=f"Target: {'≤' if lower_is_better else '≥'}{threshold}")
        return

    color = "🟢" if hits >= 5 else "🟡" if hits >= 3 else "🔴"
    avg_fmt = f"{avg_val:.1f}{unit}" if avg_val and avg_val == avg_val else "—"
    st.metric(
        label=f"{color} {label}",
        value=f"{hits}/{total} days",
        delta=f"avg {avg_fmt}",
        delta_color="off",
        help=f"Target: {'≤' if lower_is_better else '≥'}{threshold}{unit}. This week Mon–today.",
    )


def macrocycle_bar(label: str, current, target: float, unit: str,
                   lower_is_better: bool = False):
    """
    Render a macrocycle progress bar.
    Bar fill = how close current is to target as a fraction of target.
    Shows the gap remaining rather than a misleading percentage.
    """
    if current is None or current != current:
        st.markdown(f"**{label}** — no data")
        return
    val   = float(current)
    gap   = val - target
    arrow = "▼" if lower_is_better else "▲"

    if lower_is_better:
        pct = max(0.0, min(1.0, target / val if val > 0 else 0))
        gap_str = f"{abs(gap):.1f}{unit} to lose" if gap > 0 else "✓ at target"
    else:
        pct = max(0.0, min(1.0, val / target if target > 0 else 0))
        gap_str = f"{abs(gap):.1f}{unit} to go" if gap < 0 else "✓ at target"

    bar_filled = int(pct * 20)
    bar        = "█" * bar_filled + "░" * (20 - bar_filled)

    st.markdown(
        f"**{label}** &nbsp; `{bar}` &nbsp; "
        f"{val:.1f}{unit} → {arrow}{target:.0f}{unit} &nbsp; "
        f"<span style='opacity:0.55'>{gap_str}</span>",
        unsafe_allow_html=True,
    )


def trend_chart(df: pd.DataFrame, col: str, rolling_col: str, title: str, unit: str = ""):
    """Line chart: raw daily values (faint) + 7-day rolling average (bold)."""
    if df.empty or rolling_col not in df.columns:
        st.info(f"No data for {title} yet.")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df[col],
        mode="markers", name="Daily",
        marker=dict(size=4, opacity=0.35),
        hovertemplate=f"%{{x|%b %d}}: %{{y:.1f}}{unit}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=df["date"], y=df[rolling_col],
        mode="lines", name="7-day avg",
        line=dict(width=2.5),
        hovertemplate=f"%{{x|%b %d}} 7d avg: %{{y:.1f}}{unit}<extra></extra>",
    ))
    fig.update_layout(
        title=title,
        height=260,
        margin=dict(l=0, r=0, t=36, b=0),
        legend=dict(orientation="h", y=1.15),
        xaxis=dict(showgrid=False),
        yaxis=dict(gridcolor="rgba(128,128,128,0.15)"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)


def strength_chart(lift_df: pd.DataFrame, movement_aliases: list[str], title: str):
    """Scatter + smoothed line for estimated 1RM over time."""
    if lift_df.empty:
        return
    mask = lift_df["movement"].str.lower().apply(
        lambda m: any(alias in m for alias in movement_aliases)
    )
    sub = lift_df[mask].sort_values("date")
    if sub.empty:
        st.info(f"No {title} data yet.")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=sub["date"], y=sub["est_1rm"],
        mode="markers+lines", name="Est. 1RM",
        line=dict(width=2),
        marker=dict(size=5),
        hovertemplate="%{x|%b %d}: %{y:.0f} lbs<extra></extra>",
    ))
    fig.update_layout(
        title=f"{title} — Est. 1RM",
        height=220,
        margin=dict(l=0, r=0, t=36, b=0),
        xaxis=dict(showgrid=False),
        yaxis=dict(gridcolor="rgba(128,128,128,0.15)", ticksuffix=" lbs"),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════
#  Fitbit OAuth Callback Handler (runs at page load)
# ══════════════════════════════════════════════════════════════

def handle_oauth_callback():
    """
    If Fitbit redirected back to this app with ?code=..., exchange it for tokens.
    Streamlit re-renders on every navigation, so this runs on each page load.
    """
    params = st.query_params
    if "code" not in params:
        return
    code = params["code"]
    tokens = exchange_fitbit_code(code)
    if tokens:
        save_tokens(tokens)
        st.query_params.clear()
        st.success("✅ Fitbit connected! Token stored.")
        st.cache_data.clear()
    else:
        st.error("❌ Failed to exchange Fitbit code. Try again.")


# ══════════════════════════════════════════════════════════════
#  Sidebar Token Status
# ══════════════════════════════════════════════════════════════

def render_fitbit_status():
    """Show Fitbit connection status + re-auth button in sidebar."""
    row = load_tokens()
    if not row:
        st.sidebar.warning("Fitbit not connected")
        st.sidebar.link_button("🔗 Connect Fitbit", fitbit_auth_url())
        return

    expires_at = datetime.fromisoformat(row["expires_at"])
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    if datetime.now(timezone.utc) < expires_at:
        st.sidebar.success(f"Fitbit connected\nExpires {expires_at.strftime('%b %d %H:%M UTC')}")
    else:
        st.sidebar.error("Fitbit token expired")
        st.sidebar.link_button("🔗 Re-authorize Fitbit", fitbit_auth_url())


# ══════════════════════════════════════════════════════════════
#  Pages
# ══════════════════════════════════════════════════════════════

def page_dashboard():
    st.header("Dashboard")

    df       = load_rolling_averages()
    lift_df  = load_lifting_log()
    avgs     = current_rolling_avgs(df)

    squat_1rm    = best_1rm(lift_df, STRENGTH_MOVEMENTS["squat"])
    bench_1rm    = best_1rm(lift_df, STRENGTH_MOVEMENTS["bench"])
    deadlift_1rm = best_1rm(lift_df, STRENGTH_MOVEMENTS["deadlift"])
    recent_bw    = float(avgs["weight_lbs"]) if avgs.get("weight_lbs") else None
    pft          = marine_readiness_score(lift_df, recent_bw)
    prev         = prev_rolling_avgs(df)

    # Marine PFT section hidden until Phase 4 (run data + plank logging established)

    # ── Contingency Protocol ─────────────────────────────────
    st.subheader("Contingency Protocol")
    raw_df = df.sort_values("date")

    import zoneinfo
    tz          = zoneinfo.ZoneInfo("America/New_York")
    today_local = pd.Timestamp.now(tz=tz).normalize().tz_localize(None)
    day_num     = today_local.dayofweek + 1        # Mon=1 … Sun=7

    cur_week_start  = today_local - pd.Timedelta(days=today_local.dayofweek)
    cur_week_end    = today_local
    last_week_start = cur_week_start  - pd.Timedelta(days=7)
    last_week_end   = cur_week_start  - pd.Timedelta(days=1)
    prior_week_start= last_week_start - pd.Timedelta(days=7)
    prior_week_end  = last_week_start - pd.Timedelta(days=1)

    st.caption(
        f"{today_local.strftime('%a %b %-d')} — "
        f"Day {day_num} of current week "
        f"({cur_week_start.strftime('%a %-m/%-d')} – {cur_week_end.strftime('%a %b %-d')}). "
        f"Showing average for the last completed week "
        f"({last_week_start.strftime('%a %-m/%-d')} – {last_week_end.strftime('%a %b %-d')})."
    )

    def _week_avg(col, start, end):
        mask = (raw_df["date"] >= start) & (raw_df["date"] <= end)
        sub  = raw_df[mask].dropna(subset=[col])
        return round(float(sub[col].mean()), 2) if not sub.empty else None

    # Last completed week averages
    lw_weight = _week_avg("weight_lbs",  last_week_start, last_week_end)
    lw_sleep  = _week_avg("sleep_hours", last_week_start, last_week_end)
    lw_steps  = _week_avg("step_count",  last_week_start, last_week_end)

    # Prior week averages (for green/red direction comparison)
    pw_weight = _week_avg("weight_lbs",  prior_week_start, prior_week_end)
    pw_sleep  = _week_avg("sleep_hours", prior_week_start, prior_week_end)
    pw_steps  = _week_avg("step_count",  prior_week_start, prior_week_end)

    def _contingency_tile(label, lw_val, pw_val, target, unit, lower_is_better=False):
        """
        Show last completed week's average.
        Green  = trend moving in the right direction vs prior week.
        Red    = trend moving in the wrong direction.
        Delta  = change from prior week (not vs target).
        Help   = distance remaining to goal target.
        """
        if lw_val is None:
            st.metric(f"⚪ {label}", "—")
            return

        # Direction of change vs prior week
        if pw_val is not None and pw_val > 0:
            change = lw_val - pw_val
            improved = (change <= 0) if lower_is_better else (change >= 0)
            color = "🟢" if improved else "🔴"
            if lower_is_better:
                delta_str = f"{'▼' if change < 0 else '▲'} {abs(change):.1f}{unit} vs prior week"
            else:
                delta_str = f"{'▲' if change > 0 else '▼'} {abs(change):.1f}{unit} vs prior week"
            delta_color = "normal" if improved else "inverse"
        else:
            color = "⚪"
            delta_str = "no prior week data"
            delta_color = "off"

        # Gap to goal for the help tooltip
        gap = lw_val - target
        if lower_is_better:
            gap_label = f"{abs(gap):.1f}{unit} to lose" if gap > 0 else "✓ at goal"
        else:
            gap_label = f"{abs(gap):.1f}{unit} to go" if gap < 0 else "✓ at goal"

        # Format value
        if unit == "" and isinstance(lw_val, float):
            fmt = f"{int(round(lw_val)):,}"
        else:
            fmt = f"{lw_val:.1f}{unit}"

        st.metric(
            label=f"{color} {label}",
            value=fmt,
            delta=delta_str,
            delta_color=delta_color,
            help=f"Goal: {'≤' if lower_is_better else '≥'}{target}{unit} · {gap_label}",
        )

    col1, col2, col3 = st.columns(3)
    with col1:
        _contingency_tile("Weight", lw_weight, pw_weight, TARGETS["weight_lbs"], " lbs", lower_is_better=True)
    with col2:
        _contingency_tile("Sleep",  lw_sleep,  pw_sleep,  TARGETS["sleep_hours"], "h")
    with col3:
        _contingency_tile("Steps",  lw_steps,  pw_steps,  TARGETS["step_count"],  "")

    st.divider()

    # ── Macrocycle progress ────────────────────────────────────
    st.subheader("Macrocycle Progress")
    macrocycle_bar("Weight",   avgs.get("avg_weight_7d"),   TARGETS["weight_lbs"],   " lbs", lower_is_better=True)
    # For strength, also show recent trend (last 30d best vs prior 30d best)
    def _strength_bar(label, key, aliases, target):
        current = best_1rm(lift_df, aliases)
        prior   = best_1rm(
            lift_df[lift_df["date"] < pd.Timestamp.now() - pd.Timedelta(days=30)], aliases
        ) if not lift_df.empty else None
        macrocycle_bar(label, current, target, " lbs")
        if current and prior and prior > 0:
            delta = current - prior
            trend = f"{'▲' if delta >= 0 else '▼'} {abs(delta):.0f} lbs vs 30d ago"
            st.caption(trend)

    _strength_bar("Squat",    "squat",    STRENGTH_MOVEMENTS["squat"],    TARGETS["squat_1rm"])
    _strength_bar("Bench",    "bench",    STRENGTH_MOVEMENTS["bench"],    TARGETS["bench_1rm"])
    _strength_bar("Deadlift", "deadlift", STRENGTH_MOVEMENTS["deadlift"], TARGETS["deadlift_1rm"])
    # PFT macrocycle bar — target 270 (1st Class)
    macrocycle_bar("PFT Score", float(pft["total"]) if pft["total"] else None, 270.0, " pts")

    st.divider()

    # ── Biometric trends ───────────────────────────────────────
    st.subheader("Trends")
    c1, c2 = st.columns(2)
    with c1:
        trend_chart(raw_df, "weight_lbs", "avg_weight_7d", "Weight", " lbs")
    with c2:
        trend_chart(raw_df, "sleep_hours", "avg_sleep_7d", "Sleep", "h")

    c3, c4 = st.columns(2)
    with c3:
        trend_chart(raw_df, "step_count", "avg_steps_7d", "Steps")
    with c4:
        trend_chart(raw_df, "calories", "avg_calories_7d", "Calories burned", " kcal")

    st.divider()

    # ── Strength progression ───────────────────────────────────
    st.subheader("Strength Progression")
    col_s, col_b, col_d = st.columns(3)
    with col_s:
        st.metric("Squat 1RM",    f"{squat_1rm:.0f} lbs"    if squat_1rm    else "—")
    with col_b:
        st.metric("Bench 1RM",    f"{bench_1rm:.0f} lbs"    if bench_1rm    else "—")
    with col_d:
        st.metric("Deadlift 1RM", f"{deadlift_1rm:.0f} lbs" if deadlift_1rm else "—")

    s1, s2, s3 = st.columns(3)
    with s1:
        strength_chart(lift_df, STRENGTH_MOVEMENTS["squat"],    "Squat")
    with s2:
        strength_chart(lift_df, STRENGTH_MOVEMENTS["bench"],    "Bench")
    with s3:
        strength_chart(lift_df, STRENGTH_MOVEMENTS["deadlift"], "Deadlift")


def page_sync():
    st.header("Sync & Upload")

    # ── Fitbit pull ────────────────────────────────────────────
    st.subheader("Fitbit — Pull Latest Week")
    if st.button("🔄 Sync last 7 days from Fitbit", type="primary"):
        token = get_valid_access_token()
        if not token:
            st.error("Fitbit not authorized. Use the sidebar to connect.")
        else:
            with st.spinner("Fetching from Fitbit..."):
                df = fetch_fitbit_week(token)
            if df.empty:
                st.warning("No data returned from Fitbit for the past 7 days.")
            else:
                df = df.where(pd.notnull(df), other=None)
                records = [
                    {k: (None if isinstance(v, float) and v != v else v) for k, v in row.items()}
                    for row in df.to_dict("records")
                ]
                get_supabase().table("daily_metrics").upsert(
                    records, on_conflict="date"
                ).execute()
                st.success(f"✅ Synced {len(df)} days.")
                st.cache_data.clear()
                st.dataframe(df, use_container_width=True)

    st.divider()

    # ── Hevy CSV upload ────────────────────────────────────────
    st.subheader("Upload Hevy Workout CSV")
    st.caption("Export from Hevy → Profile → Export Data → CSV")
    uploaded = st.file_uploader("Hevy export (.csv)", type="csv")

    if uploaded:
        raw = pd.read_csv(io.StringIO(uploaded.read().decode("utf-8")))
        try:
            processed = process_hevy_df(raw)
        except Exception as ex:
            st.error(f"Failed to parse CSV: {ex}")
            st.stop()

        st.write(f"**{len(processed)} records** across **{processed['movement'].nunique()} movements**")

        # Preview best sets for key lifts
        key_aliases = STRENGTH_MOVEMENTS["squat"] + STRENGTH_MOVEMENTS["bench"] + STRENGTH_MOVEMENTS["deadlift"]
        preview = processed[
            processed["movement"].str.lower().apply(
                lambda m: any(a in m for a in key_aliases)
            )
        ].sort_values("date", ascending=False).head(15)
        if not preview.empty:
            st.write("Key lift preview:")
            st.dataframe(
                preview[["date", "movement", "weight_lbs", "reps", "est_1rm", "sets"]],
                use_container_width=True,
            )

        if st.button("⬆️ Upsert into database", type="primary"):
            records = []
            for _, row in processed.iterrows():
                records.append({
                    "date":       str(row["date"]),
                    "movement":   str(row["movement"]),
                    "weight_lbs": float(row["weight_lbs"]),
                    "sets":       int(row.get("sets", 1)),
                    "reps":       int(row["reps"]),
                    "est_1rm":    float(row["est_1rm"]),
                })
            with st.spinner("Upserting..."):
                for i in range(0, len(records), 500):
                    get_supabase().table("lifting_log").upsert(
                        records[i : i + 500], on_conflict="date,movement"
                    ).execute()
            st.success(f"✅ {len(records)} records upserted.")
            st.cache_data.clear()

    st.divider()

    # ── Manual data correction ─────────────────────────────────
    st.subheader("Manual Data Correction")
    st.caption("Override a specific day's macros or biometrics. ≤ 3 clicks.")

    with st.form("correction_form"):
        target_date = st.date_input("Date to correct", value=date.today() - timedelta(days=1))
        c1, c2, c3, c4, c5 = st.columns(5)
        weight   = c1.number_input("Weight (lbs)", min_value=0.0, step=0.1, value=0.0, format="%.1f")
        sleep    = c2.number_input("Sleep (hrs)",  min_value=0.0, step=0.25, value=0.0, format="%.2f")
        steps    = c3.number_input("Steps",        min_value=0,   step=100,  value=0)
        calories = c4.number_input("Calories",     min_value=0,   step=10,   value=0)
        protein  = c5.number_input("Protein (g)",  min_value=0,   step=1,    value=0)
        submitted = st.form_submit_button("💾 Save correction", type="primary")

    if submitted:
        row: dict = {"date": target_date.isoformat()}
        if weight   > 0:   row["weight_lbs"]  = weight
        if sleep    > 0:   row["sleep_hours"]  = sleep
        if steps    > 0:   row["step_count"]   = steps
        if calories > 0:   row["calories"]     = calories
        if protein  > 0:   row["protein_g"]    = protein
        if len(row) == 1:
            st.warning("No values entered — nothing saved.")
        else:
            get_supabase().table("daily_metrics").upsert(row, on_conflict="date").execute()
            st.success(f"✅ Corrected {target_date.isoformat()} with {list(row.keys())[1:]}")
            st.cache_data.clear()


# ══════════════════════════════════════════════════════════════
#  App Entry Point
# ══════════════════════════════════════════════════════════════

def main():
    st.set_page_config(
        page_title="PulkFit 2.0",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Handle Fitbit redirect callback (captures ?code= from URL)
    handle_oauth_callback()

    # Sidebar navigation
    with st.sidebar:
        st.title("PulkFit 2.0")
        page = st.radio("", ["Dashboard", "Sync & Upload"], label_visibility="collapsed")
        st.divider()
        render_fitbit_status()

    if page == "Dashboard":
        page_dashboard()
    elif page == "Sync & Upload":
        page_sync()


if __name__ == "__main__":
    main()
