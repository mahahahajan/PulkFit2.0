#!/usr/bin/env python3
"""
PulkFit 2.0 — Phase 1: Historical Bootstrap
============================================
Run this ONCE locally to seed your Supabase database with
6 months of Fitbit history and your Hevy workout logs.

Prerequisites:
  1. pip install -r requirements.txt
  2. Copy .env.example → .env and fill in all values
  3. Your Supabase schema must already be applied (schema.sql)

Usage:
  python phase1_bootstrap.py
  python phase1_bootstrap.py --hevy /path/to/hevy_export.csv
  python phase1_bootstrap.py --months 3   # shorter history window
"""

import argparse
import os
import sys
import time
import webbrowser
import urllib.parse
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

import pandas as pd
import requests
from dotenv import load_dotenv
from supabase import Client, create_client

load_dotenv()

# ── Configuration ──────────────────────────────────────────────────────────────

SUPABASE_URL         = os.environ["SUPABASE_URL"]
SUPABASE_SECRET_KEY  = os.environ["SUPABASE_SECRET_KEY"]
FITBIT_CLIENT_ID    = os.environ["FITBIT_CLIENT_ID"]
FITBIT_CLIENT_SECRET = os.environ["FITBIT_CLIENT_SECRET"]
FITBIT_REDIRECT_URI = "http://localhost:8081/"
FITBIT_SCOPE        = "weight sleep activity"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)


# ── Fitbit OAuth Flow ──────────────────────────────────────────────────────────

_auth_code: str | None = None


class _OAuthHandler(BaseHTTPRequestHandler):
    """Minimal HTTP handler to capture the OAuth redirect code."""

    def do_GET(self):
        global _auth_code
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if "code" in params:
            _auth_code = params["code"][0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(
                b"<h2 style='font-family:sans-serif'>Authorized! You may close this tab.</h2>"
            )
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"<h2>Error: no code in redirect. Try again.</h2>")

    def log_message(self, *args):
        pass  # suppress server logs


def fitbit_oauth() -> dict:
    """
    Open Fitbit authorization in browser, spin up a local server to capture
    the redirect, then exchange the code for tokens. Returns the token dict.
    """
    base_params = urllib.parse.urlencode({
        "response_type": "code",
        "client_id":     FITBIT_CLIENT_ID,
        "scope":         FITBIT_SCOPE,
        "expires_in":    "604800",
    })
    auth_url = (
        f"https://www.fitbit.com/oauth2/authorize?{base_params}"
        f"&redirect_uri={FITBIT_REDIRECT_URI}"
    )

    print("\n📡  Opening Fitbit authorization in your browser...")
    print(f"    If it doesn't open automatically, visit:\n    {auth_url}\n")

    def _serve_until_code():
        while not _auth_code:
            server.handle_request()

    server = HTTPServer(("localhost", 8081), _OAuthHandler)
    thread = Thread(target=_serve_until_code)
    thread.start()
    webbrowser.open(auth_url)
    thread.join(timeout=120)
    server.server_close()

    if not _auth_code:
        print("❌  OAuth timed out after 120s. Please rerun the script.")
        sys.exit(1)

    resp = requests.post(
        "https://api.fitbit.com/oauth2/token",
        data={
            "grant_type":   "authorization_code",
            "code":          _auth_code,
            "redirect_uri":  FITBIT_REDIRECT_URI,
        },
        auth=(FITBIT_CLIENT_ID, FITBIT_CLIENT_SECRET),
        timeout=15,
    )
    resp.raise_for_status()
    tokens = resp.json()
    print("✅  Fitbit authorized.")
    return tokens


def store_tokens(tokens: dict):
    """Persist OAuth tokens in Supabase for app.py to use at runtime."""
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=tokens["expires_in"])
    supabase.table("auth_tokens").upsert(
        {
            "service":       "fitbit",
            "access_token":  tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "expires_at":    expires_at.isoformat(),
        },
        on_conflict="service",
    ).execute()
    print("✅  Tokens stored in Supabase.")


# ── Fitbit Data Fetching ───────────────────────────────────────────────────────

def _fitbit_get(access_token: str, endpoint: str) -> dict:
    resp = requests.get(
        f"https://api.fitbit.com{endpoint}",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _date_chunks(start: date, end: date, days: int = 30):
    """Yield (chunk_start, chunk_end) pairs to paginate a date range."""
    cur = start
    while cur <= end:
        yield cur, min(cur + timedelta(days=days - 1), end)
        cur += timedelta(days=days)


def fetch_fitbit_history(access_token: str, months: int) -> pd.DataFrame:
    """
    Pull weight, sleep, and steps for the past `months` months from Fitbit.
    Returns a DataFrame with columns: date, weight_lbs, sleep_hours, step_count.
    """
    end_dt   = date.today() - timedelta(days=1)
    start_dt = end_dt - timedelta(days=months * 30)

    print(f"\n📥  Fetching {months}mo of Fitbit data ({start_dt} → {end_dt})...")

    records: dict[str, dict] = {}

    for chunk_start, chunk_end in _date_chunks(start_dt, end_dt):
        s, e = chunk_start.isoformat(), chunk_end.isoformat()
        print(f"    {s} → {e}", end="", flush=True)

        # Weight
        try:
            data = _fitbit_get(access_token, f"/1/user/-/body/weight/date/{s}/{e}.json")
            for entry in data.get("body-weight", []):
                records.setdefault(entry["dateTime"], {})["weight_lbs"] = round(float(entry["value"]) * 2.20462, 1)
            print(" w✓", end="", flush=True)
        except Exception as ex:
            print(f" w✗({ex})", end="", flush=True)

        # Sleep
        try:
            data = _fitbit_get(access_token, f"/1.2/user/-/sleep/date/{s}/{e}.json")
            for entry in data.get("sleep", []):
                d = entry["dateOfSleep"]
                hours = entry.get("minutesAsleep", 0) / 60
                # If there are multiple sleep logs on one day, sum them
                prev = records.setdefault(d, {}).get("sleep_hours", 0.0)
                records[d]["sleep_hours"] = round(prev + hours, 2)
            print(" s✓", end="", flush=True)
        except Exception as ex:
            print(f" s✗({ex})", end="", flush=True)

        # Steps
        try:
            data = _fitbit_get(access_token, f"/1/user/-/activities/steps/date/{s}/{e}.json")
            for entry in data.get("activities-steps", []):
                records.setdefault(entry["dateTime"], {})["step_count"] = int(entry["value"])
            print(" st✓", end="", flush=True)
        except Exception as ex:
            print(f" st✗({ex})", end="", flush=True)

        # Calories burned
        try:
            data = _fitbit_get(access_token, f"/1/user/-/activities/calories/date/{s}/{e}.json")
            for entry in data.get("activities-calories", []):
                records.setdefault(entry["dateTime"], {})["calories"] = int(float(entry["value"]))
            print(" cal✓", end="", flush=True)
        except Exception as ex:
            print(f" cal✗({ex})", end="", flush=True)

        print()
        time.sleep(0.4)  # polite rate limit buffer

    rows = [{"date": d, **v} for d, v in records.items()]
    df = pd.DataFrame(rows)
    print(f"✅  {len(df)} days of Fitbit data ready.\n")
    return df


# ── Hevy CSV Processing ────────────────────────────────────────────────────────

def brzycki_1rm(weight_lbs: float, reps: int) -> float:
    """
    Brzycki formula: 1RM = weight × (36 / (37 - reps))
    Returns weight unchanged for reps >= 37 or weight == 0.
    """
    if reps < 1 or reps >= 37 or weight_lbs <= 0:
        return round(weight_lbs, 1)
    return round(weight_lbs * (36 / (37 - reps)), 1)


# Hevy CSV column name variants across export versions
_HEVY_COL_MAP = {
    "exercise_title":      "movement",
    "exercise_name":       "movement",
    "title":               "workout_name",      # workout name, not exercise — ignored
    "start_time":          "date_raw",
    "workout_start_time":  "date_raw",
    "date":                "date_raw",
    "weight_(lbs)":        "weight_lbs",
    "weight_lbs":          "weight_lbs",
    "weight":              "weight_lbs",
    "reps":                "reps",
    "set_index":           "set_num",
    "set_order":           "set_num",
}

_KEY_MOVEMENTS = {"squat", "bench", "deadlift"}  # used to validate import


def process_hevy_csv(filepath: str) -> pd.DataFrame:
    """
    Parse a Hevy workout export CSV.
    Returns one row per (date, movement) with columns:
        date, movement, weight_lbs, reps, est_1rm, sets
    The row chosen per group is the one with the highest est_1rm (best set).
    """
    print(f"📥  Loading Hevy CSV: {filepath}")
    raw = pd.read_csv(filepath, low_memory=False)
    raw.columns = [c.strip().lower().replace(" ", "_") for c in raw.columns]
    raw = raw.rename(columns={k: v for k, v in _HEVY_COL_MAP.items() if k in raw.columns})

    # ── Date parsing ─────────────────────────────────────────
    if "date_raw" not in raw.columns:
        raise ValueError(
            "Cannot find a date column in the Hevy CSV. "
            "Expected one of: start_time, workout_start_time, date."
        )
    raw["date"] = pd.to_datetime(raw["date_raw"], utc=True, errors="coerce").dt.date.astype(str)

    # ── Coerce numerics ───────────────────────────────────────
    raw["weight_lbs"] = pd.to_numeric(raw.get("weight_lbs", 0), errors="coerce").fillna(0.0)
    raw["reps"]       = pd.to_numeric(raw.get("reps",       0), errors="coerce").fillna(0).astype(int)

    if "movement" not in raw.columns:
        raise ValueError("Cannot find exercise name column. Expected: exercise_title or exercise_name.")

    raw["movement"] = raw["movement"].str.strip()

    # ── Calculate 1RM per set ─────────────────────────────────
    raw["est_1rm"] = raw.apply(
        lambda r: brzycki_1rm(r["weight_lbs"], r["reps"]), axis=1
    )

    # ── Best set per (date, movement) ─────────────────────────
    best_idx = raw.groupby(["date", "movement"])["est_1rm"].idxmax()
    best = raw.loc[best_idx, ["date", "movement", "weight_lbs", "reps", "est_1rm"]].copy()

    # ── Set count per (date, movement) ────────────────────────
    set_counts = raw.groupby(["date", "movement"]).size().reset_index(name="sets")
    best = best.merge(set_counts, on=["date", "movement"], how="left")

    # ── Quick validation ──────────────────────────────────────
    found_movements = set(best["movement"].str.lower())
    found_key = {m for m in _KEY_MOVEMENTS if any(m in mv for mv in found_movements)}
    print(f"✅  {len(best)} records · {best['movement'].nunique()} unique movements")
    if found_key:
        print(f"    Key lifts detected: {', '.join(sorted(found_key))}")
    else:
        print("    ⚠️   No squat/bench/deadlift detected — check exercise names if unexpected.")

    return best


# ── Supabase Upsert Helpers ────────────────────────────────────────────────────

_BATCH = 500


def upsert_daily_metrics(df: pd.DataFrame):
    """Upsert daily_metrics rows, replacing on date conflict."""
    print(f"\n⬆️   Upserting {len(df)} rows → daily_metrics")
    # Convert all NaN/NaT to None so JSON serialization doesn't fail
    df = df.where(pd.notnull(df), other=None)
    records = []
    for row in df.to_dict("records"):
        records.append({k: (None if isinstance(v, float) and v != v else v)
                        for k, v in row.items()})
    for i in range(0, len(records), _BATCH):
        batch = records[i : i + _BATCH]
        supabase.table("daily_metrics").upsert(batch, on_conflict="date").execute()
        print(f"    batch {i // _BATCH + 1}: {len(batch)} rows ✓")
    print("✅  daily_metrics done.")


def upsert_lifting_log(df: pd.DataFrame):
    """Upsert lifting_log rows, replacing on (date, movement) conflict."""
    print(f"\n⬆️   Upserting {len(df)} rows → lifting_log")
    records = []
    for _, row in df.iterrows():
        records.append(
            {
                "date":       str(row["date"]),
                "movement":   str(row["movement"]),
                "weight_lbs": float(row["weight_lbs"]),
                "sets":       int(row.get("sets", 1)),
                "reps":       int(row["reps"]),
                "est_1rm":    float(row["est_1rm"]),
            }
        )
    for i in range(0, len(records), _BATCH):
        batch = records[i : i + _BATCH]
        supabase.table("lifting_log").upsert(
            batch, on_conflict="date,movement"
        ).execute()
        print(f"    batch {i // _BATCH + 1}: {len(batch)} rows ✓")
    print("✅  lifting_log done.")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="PulkFit 2.0 — Historical Bootstrap")
    parser.add_argument("--hevy",   metavar="FILE", help="Path to Hevy export CSV")
    parser.add_argument("--months", metavar="N", type=int, default=6,
                        help="Months of Fitbit history to import (default: 6)")
    parser.add_argument("--skip-fitbit", action="store_true",
                        help="Skip Fitbit OAuth and data pull")
    args = parser.parse_args()

    print("=" * 60)
    print("  PulkFit 2.0  ·  Phase 1: Historical Bootstrap")
    print("=" * 60)

    # ── Step 1: Fitbit ────────────────────────────────────────
    if not args.skip_fitbit:
        tokens = fitbit_oauth()
        store_tokens(tokens)
        fitbit_df = fetch_fitbit_history(tokens["access_token"], months=args.months)
        upsert_daily_metrics(fitbit_df)
    else:
        print("\n⏭️   Skipping Fitbit pull (--skip-fitbit)")

    # ── Step 2: Hevy CSV ──────────────────────────────────────
    hevy_path = args.hevy
    if not hevy_path:
        hevy_path = input("\n📂  Path to Hevy export CSV (Enter to skip): ").strip()

    if hevy_path:
        if not os.path.isfile(hevy_path):
            print(f"⚠️   File not found: {hevy_path}. Skipping Hevy import.")
        else:
            hevy_df = process_hevy_csv(hevy_path)
            upsert_lifting_log(hevy_df)
    else:
        print("⏭️   Skipping Hevy import.")

    print("\n" + "=" * 60)
    print("  ✅  Bootstrap complete!")
    print("  Next step: deploy app.py to Streamlit Community Cloud.")
    print("  See README for Phase 2 instructions.")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
