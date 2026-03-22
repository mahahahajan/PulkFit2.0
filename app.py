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
    "weight_lbs":   float(_env("TARGET_WEIGHT_LBS",   "185.0")),
    "sleep_hours":  float(_env("TARGET_SLEEP_HOURS",  "7.5")),
    "step_count":   int(_env("TARGET_STEP_COUNT",      "10000")),
    "calories":     int(_env("TARGET_CALORIES",        "2400")),
    "protein_g":    int(_env("TARGET_PROTEIN_G",       "180")),
    "squat_1rm":    float(_env("TARGET_SQUAT_1RM",     "315")),
    "bench_1rm":    float(_env("TARGET_BENCH_1RM",     "225")),
    "deadlift_1rm": float(_env("TARGET_DEADLIFT_1RM",  "405")),
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
    return create_client(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY)


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
        records.setdefault(entry["dateTime"], {})["weight_lbs"] = float(entry["value"])

    for entry in _get(f"/1.2/user/-/sleep/date/{s}/{e}.json").get("sleep", []):
        d     = entry["dateOfSleep"]
        hours = entry.get("minutesAsleep", 0) / 60
        prev  = records.setdefault(d, {}).get("sleep_hours", 0.0)
        records[d]["sleep_hours"] = round(prev + hours, 2)

    for entry in _get(f"/1/user/-/activities/steps/date/{s}/{e}.json").get("activities-steps", []):
        records.setdefault(entry["dateTime"], {})["step_count"] = int(entry["value"])

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
#  Marine Readiness Score
# ══════════════════════════════════════════════════════════════

def marine_readiness_score(avgs: dict, squat_1rm: float | None,
                            bench_1rm: float | None, deadlift_1rm: float | None) -> float:
    """
    Composite 0–100 score across four pillars, 25 pts each:
      1. Steps    — avg_steps_7d vs TARGET
      2. Sleep    — avg_sleep_7d vs TARGET
      3. Strength — (squat + bench + deadlift) vs TARGET total
      4. Protein  — avg_protein_7d vs TARGET
    """
    def pct(val, target) -> float:
        if val is None or target == 0:
            return 0.0
        return min(float(val) / target, 1.0) * 25

    steps_score   = pct(avgs.get("avg_steps_7d"),  TARGETS["step_count"])
    sleep_score   = pct(avgs.get("avg_sleep_7d"),  TARGETS["sleep_hours"])
    protein_score = pct(avgs.get("avg_protein_7d"), TARGETS["protein_g"])

    target_total = TARGETS["squat_1rm"] + TARGETS["bench_1rm"] + TARGETS["deadlift_1rm"]
    actual_total = (squat_1rm or 0) + (bench_1rm or 0) + (deadlift_1rm or 0)
    strength_score = pct(actual_total, target_total)

    return round(steps_score + sleep_score + protein_score + strength_score, 1)


# ══════════════════════════════════════════════════════════════
#  UI Components
# ══════════════════════════════════════════════════════════════

def indicator(label: str, current, target, unit: str = "", lower_is_better: bool = False):
    """
    Render a metric tile with a Red / Green status dot.
    Green = within 5% of target (or past it for weight-loss targets).
    """
    if current is None or current != current:  # NaN check
        st.metric(label, "—")
        return

    delta_pct = (float(current) - target) / target if target else 0
    if lower_is_better:
        green = delta_pct <= 0.05  # at or below target
    else:
        green = delta_pct >= -0.05  # within 5% of or above target

    color = "🟢" if green else "🔴"
    formatted = f"{current:,.1f}{unit}" if isinstance(current, float) else f"{int(current):,}{unit}"
    target_fmt = f"{target:,.1f}{unit}" if isinstance(target, float) else f"{int(target):,}{unit}"
    delta_str  = f"{delta_pct:+.1%} vs target {target_fmt}"

    st.metric(label=f"{color} {label}", value=formatted, delta=delta_str)


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
    mrs          = marine_readiness_score(avgs, squat_1rm, bench_1rm, deadlift_1rm)

    # ── Marine Readiness Score banner ─────────────────────────
    score_color = "#22c55e" if mrs >= 75 else "#f59e0b" if mrs >= 50 else "#ef4444"
    st.markdown(
        f"""<div style='padding:16px 20px;border-radius:10px;
                border:1px solid {score_color}40;background:{score_color}10;
                display:flex;align-items:center;gap:16px;margin-bottom:16px'>
            <span style='font-size:2.4rem;font-weight:500;color:{score_color}'>{mrs}</span>
            <div>
              <div style='font-size:0.9rem;font-weight:500;color:{score_color}'>Marine Readiness Score</div>
              <div style='font-size:0.75rem;opacity:0.7'>steps · sleep · protein · strength</div>
            </div>
        </div>""",
        unsafe_allow_html=True,
    )

    # ── Contingency Protocol ───────────────────────────────────
    st.subheader("Contingency Protocol")
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        indicator("Weight", avgs.get("avg_weight_7d"), TARGETS["weight_lbs"], " lbs",
                  lower_is_better=True)
    with col2:
        indicator("Sleep",  avgs.get("avg_sleep_7d"),  TARGETS["sleep_hours"], "h")
    with col3:
        indicator("Steps",  avgs.get("avg_steps_7d"),  TARGETS["step_count"])
    with col4:
        indicator("Calories", avgs.get("avg_calories_7d"), TARGETS["calories"], " kcal")
    with col5:
        indicator("Protein", avgs.get("avg_protein_7d"), TARGETS["protein_g"], "g")

    st.divider()

    # ── Biometric trends ───────────────────────────────────────
    st.subheader("7-Day Rolling Trends")
    c1, c2 = st.columns(2)
    with c1:
        trend_chart(df.sort_values("date"), "weight_lbs", "avg_weight_7d", "Weight", " lbs")
    with c2:
        trend_chart(df.sort_values("date"), "sleep_hours", "avg_sleep_7d", "Sleep", "h")

    c3, c4 = st.columns(2)
    with c3:
        trend_chart(df.sort_values("date"), "step_count", "avg_steps_7d", "Steps")
    with c4:
        trend_chart(df.sort_values("date"), "protein_g",  "avg_protein_7d", "Protein", "g")

    st.divider()

    # ── Strength metrics ───────────────────────────────────────
    st.subheader("Strength Progression")
    col_s, col_b, col_d = st.columns(3)
    with col_s:
        indicator("Squat 1RM",    squat_1rm,    TARGETS["squat_1rm"],    " lbs")
    with col_b:
        indicator("Bench 1RM",    bench_1rm,    TARGETS["bench_1rm"],    " lbs")
    with col_d:
        indicator("Deadlift 1RM", deadlift_1rm, TARGETS["deadlift_1rm"], " lbs")

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
                records = df.where(pd.notnull(df), None).to_dict("records")
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
