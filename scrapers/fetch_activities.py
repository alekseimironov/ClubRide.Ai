"""
scrapers/fetch_activities.py
Fetches daily club activity feed from Strava's internal JSON API.
Output: data/real/activities.csv  (cumulative, deduplicated by Activity_ID)
Runs daily at 22:00 via scheduler.

Adapted from fetch_weekly_activities_TNCE.py — path and config changes only.
"""

import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import schedule
from dotenv import load_dotenv

# ── Paths ──────────────────────────────────────────
ROOT         = Path(__file__).parent.parent
STRAVA_DIR   = ROOT.parent
CSV_FILE     = ROOT / "data/real/activities.csv"
COOKIES_FILE = STRAVA_DIR / "strava_cookies.json"
TOKEN_FILE   = STRAVA_DIR / "strava_token.json"
CONFIG_FILE  = ROOT / "config.json"

load_dotenv(ROOT / ".env")

COLUMNS = [
    "Activity_ID", "Activity_Date", "Year", "Week_Number",
    "Athlete", "Activity_Name", "Device", "Location",
    "Distance_km", "Elev_Gain_m", "Moving_Time", "Scraped_At",
]


# ── Config & auth ──────────────────────────────────
def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)

def load_cookies():
    with open(COOKIES_FILE) as f:
        return json.load(f)

def load_token():
    with open(TOKEN_FILE) as f:
        return json.load(f)

def save_token(data):
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_valid_token():
    cfg        = load_config()
    token_data = load_token()
    if time.time() > token_data.get("expires_at", 0) - 60:
        print("🔄 Refreshing token...")
        r = requests.post("https://www.strava.com/oauth/token", data={
            "client_id":     os.getenv("STRAVA_CLIENT_ID"),
            "client_secret": os.getenv("STRAVA_CLIENT_SECRET"),
            "grant_type":    "refresh_token",
            "refresh_token": token_data["refresh_token"],
        })
        token_data = r.json()
        save_token(token_data)
    return token_data["access_token"]

def get_athlete_id(access_token):
    r = requests.get(
        "https://www.strava.com/api/v3/athlete",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10,
    )
    return r.json().get("id") if r.status_code == 200 else None


# ── Requests session ───────────────────────────────
def make_session(club_id):
    raw = load_cookies()
    s   = requests.Session()
    for c in raw:
        s.cookies.set(c["name"], c["value"], domain=c.get("domain", ".strava.com"))
    s.headers.update({
        "User-Agent":       ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/124.0.0.0 Safari/537.36"),
        "Accept":           "application/json, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer":          f"https://www.strava.com/clubs/{club_id}/recent_activity",
    })
    return s


# ── Fetch feed pages ───────────────────────────────
def fetch_activities(athlete_id, session, club_id):
    """Paginate /clubs/{id}/feed until Strava's ~60-entry cap is reached."""
    activities = []
    before     = int(time.time())
    cursor     = before
    page_n     = 0

    while True:
        page_n += 1
        r = session.get(
            f"https://www.strava.com/clubs/{club_id}/feed",
            params={
                "feed_type":  "club",
                "athlete_id": athlete_id,
                "club_id":    club_id,
                "before":     before,
                "cursor":     cursor,
            },
            timeout=15,
        )

        if r.status_code != 200:
            print(f"   HTTP {r.status_code} on page {page_n} — stopping")
            break

        try:
            entries = r.json()
            if isinstance(entries, dict):
                for key in ("entries", "activities", "data"):
                    if isinstance(entries.get(key), list):
                        entries = entries[key]
                        break
        except Exception as e:
            print(f"   JSON error page {page_n}: {e}")
            break

        if not entries or not isinstance(entries, list):
            print(f"   Empty page {page_n} — stopping")
            break

        new_cursor = None
        for entry in entries:
            act = _parse_entry(entry)
            if act:
                activities.append(act)
            cd = entry.get("cursorData")
            if isinstance(cd, dict):
                for k in ("cursor", "updated_at", "start_date"):
                    v = cd.get(k)
                    if v is not None:
                        try:
                            new_cursor = int(float(v))
                            break
                        except (TypeError, ValueError):
                            pass

        print(f"   Page {page_n}: {len([e for e in entries if _parse_entry(e)])} activities  "
              f"cursor→{new_cursor}")

        if not new_cursor or new_cursor >= cursor:
            break
        before = new_cursor
        cursor = new_cursor
        time.sleep(0.4)

    return activities


# ── Entry parsers ──────────────────────────────────
def _parse_date(val):
    if not val:
        return None
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(val).date()
        except Exception:
            return None
    s = str(val).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).date()
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt) + 6], fmt).date()
        except Exception:
            pass
    return None


def _strip_html(html_str):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", str(html_str or ""))).strip()


def _parse_stat(html_str):
    m = re.match(r"[\s]*([\d,]+\.?\d*)", str(html_str or ""))
    return float(m.group(1).replace(",", "")) if m else 0.0


def _parse_entry(entry):
    obj = entry.get("activity")
    if not isinstance(obj, dict):
        return None

    activity_id = str(obj.get("id", "")).strip()
    if not activity_id:
        return None

    act_date = _parse_date(
        obj.get("startDate") or obj.get("start_date_local") or obj.get("start_date")
    )
    if not act_date:
        return None

    ath  = obj.get("athlete", {})
    name = ""
    if isinstance(ath, dict):
        name = (ath.get("athleteName")
                or ath.get("displayName")
                or f"{ath.get('firstName', ath.get('firstname', ''))} "
                   f"{ath.get('lastName', ath.get('lastname', ''))}".strip()
                or ath.get("name", ""))
    if not name:
        return None

    # Stats
    stats       = {s["key"]: s["value"] for s in obj.get("stats", []) if "key" in s}
    distance    = elev = 0.0
    moving_time = ""
    for i in ("one", "two", "three", "four"):
        subtitle = stats.get(f"stat_{i}_subtitle", "")
        value    = stats.get(f"stat_{i}", "")
        sl       = subtitle.lower()
        if "distance" in sl:
            distance = _parse_stat(value)
        elif "elev" in sl:
            elev = _parse_stat(value)
        elif "time" in sl:
            moving_time = _strip_html(value)

    # Location
    location = ""
    tal = obj.get("timeAndLocation")
    if isinstance(tal, dict):
        location = (tal.get("location") or tal.get("locationCity") or "")
        if not location:
            display = tal.get("display") or ""
            parts   = [p.strip() for p in str(display).split("·")]
            for part in reversed(parts):
                if "," in part and len(part) > 3:
                    location = part
                    break
    elif isinstance(tal, str):
        parts = [p.strip() for p in tal.split("·")]
        for part in reversed(parts):
            if "," in part and len(part) > 3:
                location = part
                break

    # Device
    device = obj.get("deviceName", "").strip()
    if not device and isinstance(tal, dict):
        display = tal.get("display") or ""
        parts   = [p.strip() for p in str(display).split("·")]
        for part in parts[1:]:
            if part and "," not in part:
                device = part
                break

    iso = act_date.isocalendar()
    return {
        "Activity_ID":   activity_id,
        "Activity_Date": act_date.isoformat(),
        "Year":          iso[0],
        "Week_Number":   iso[1],
        "Athlete":       name,
        "Activity_Name": obj.get("activityName", "").strip(),
        "Device":        device,
        "Location":      location.strip(),
        "Distance_km":   round(distance, 2),
        "Elev_Gain_m":   int(round(elev)),
        "Moving_Time":   moving_time,
        "Scraped_At":    datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


# ── Upsert ─────────────────────────────────────────
def upsert(new_activities):
    new_df = pd.DataFrame(new_activities, columns=COLUMNS)

    try:
        existing  = pd.read_csv(CSV_FILE, dtype=str)
        known_ids = set(existing["Activity_ID"].astype(str))
        fresh     = new_df[~new_df["Activity_ID"].astype(str).isin(known_ids)]
        final     = pd.concat([existing, fresh], ignore_index=True)
        n_new     = len(fresh)
        n_dup     = len(new_df) - n_new
    except FileNotFoundError:
        final = new_df
        n_new = len(new_df)
        n_dup = 0

    final = (
        final
        .assign(
            Year=lambda d: pd.to_numeric(d["Year"], errors="coerce").astype("Int64"),
            Week_Number=lambda d: pd.to_numeric(d["Week_Number"], errors="coerce").astype("Int64"),
        )
        .sort_values(["Activity_Date", "Athlete"])
        .reset_index(drop=True)
    )

    CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(CSV_FILE, index=False)

    total_athletes = final["Athlete"].nunique()
    date_range     = f"{final['Activity_Date'].min()} → {final['Activity_Date'].max()}"
    print(f"   +{n_new} new  |  {n_dup} duplicates skipped")
    print(f"   CSV total: {len(final)} activities  "
          f"{total_athletes} unique athletes  [{date_range}]")
    return n_new


# ── Main ───────────────────────────────────────────
def run():
    print(f"\n{'='*60}")
    print(f"🚴 Daily Activities — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    cfg      = load_config()
    club_id  = cfg["club"]["club_id"]

    access_token = get_valid_token()
    athlete_id   = get_athlete_id(access_token)
    if not athlete_id:
        print("❌ Could not get athlete ID — cookies may be expired.")
        return
    print(f"   Athlete ID: {athlete_id}")

    session    = make_session(club_id)
    activities = fetch_activities(athlete_id, session, club_id)
    print(f"\n📦 Fetched {len(activities)} activities from feed")

    if not activities:
        print("❌ Nothing to save.")
        return

    n_new = upsert(activities)
    print("✅ Done." if n_new > 0 else "✅ No new activities — already up to date.")


# ── Entry point ────────────────────────────────────
if __name__ == "__main__":
    if "--now" in sys.argv:
        run()
    else:
        print("🗓️  Scheduler active — runs daily at 22:00")
        print("   Tip: python scrapers/fetch_activities.py --now")
        schedule.every().day.at("22:00").do(run)
        while True:
            schedule.run_pending()
            time.sleep(30)
