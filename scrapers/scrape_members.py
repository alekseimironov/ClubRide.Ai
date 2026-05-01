"""
scrapers/scrape_members.py
Fetches all TNCE club members from the Strava API.
Output: data/real/members.csv  (full refresh every run)

Provides the master athlete reference table:
  Athlete_ID, Full_Name, City, Country, Sex, Premium, Profile_URL

Run once to populate, then periodically to catch new joiners/leavers.
Uses the Strava API v3 — no browser/cookies needed, OAuth token only.
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# ── Paths ──────────────────────────────────────────
ROOT        = Path(__file__).parent.parent
STRAVA_DIR  = ROOT.parent
CSV_FILE    = ROOT / "data/real/members.csv"
TOKEN_FILE  = STRAVA_DIR / "strava_token.json"
CONFIG_FILE = ROOT / "config.json"

load_dotenv(ROOT / ".env")


# ── Auth ───────────────────────────────────────────
def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)

def load_token():
    with open(TOKEN_FILE) as f:
        return json.load(f)

def save_token(data):
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f, indent=2)

def get_valid_token():
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
        print("✅ Token refreshed")
    return token_data["access_token"]


# ── Fetch all members ──────────────────────────────
def fetch_members(access_token, club_id):
    """
    Paginate GET /clubs/{id}/members — returns SummaryAthlete objects.
    Strava returns up to 200 per page. Stops when page returns fewer
    than per_page entries (last page reached).
    """
    headers  = {"Authorization": f"Bearer {access_token}"}
    members  = []
    page     = 1
    per_page = 200

    while True:
        r = requests.get(
            f"https://www.strava.com/api/v3/clubs/{club_id}/members",
            headers=headers,
            params={"page": page, "per_page": per_page},
            timeout=15,
        )

        if r.status_code != 200:
            print(f"  ❌ HTTP {r.status_code}: {r.text[:200]}")
            break

        batch = r.json()
        if not batch:
            break

        members.extend(batch)
        print(f"  Page {page}: {len(batch)} members (total {len(members)})")

        if len(batch) < per_page:
            break   # last page

        page += 1
        time.sleep(0.5)

    return members


# ── Parse member ───────────────────────────────────
def parse_member(m):
    firstname = m.get("firstname", "").strip()
    lastname  = m.get("lastname", "").strip()
    full_name = f"{firstname} {lastname}".strip()

    if not full_name:
        return None     # skip — no usable name

    return {
        "Full_Name":   full_name,
        "Firstname":   firstname,
        "Lastname":    lastname,
        "Membership":  m.get("membership", "member"),
        "Admin":       m.get("admin", False),
        "Owner":       m.get("owner", False),
        "Scraped_At":  datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


# ── Main ───────────────────────────────────────────
def run():
    print(f"\n{'='*60}")
    print(f"👥 Club Members scrape — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    cfg          = load_config()
    club_id      = cfg["club"]["club_id"]
    access_token = get_valid_token()

    print(f"📡 Fetching members for club {club_id}...")
    raw_members = fetch_members(access_token, club_id)

    if not raw_members:
        print("❌ No members returned.")
        return

    parsed = [parse_member(m) for m in raw_members]
    skipped = parsed.count(None)
    df = pd.DataFrame([p for p in parsed if p is not None])
    if skipped:
        print(f"  ⚠️  {skipped} members skipped — no Athlete_ID")
    df = df.sort_values("Full_Name").reset_index(drop=True)

    # Compare with existing to show joiners / leavers
    try:
        existing      = pd.read_csv(CSV_FILE, dtype=str)
        existing_names = set(existing["Full_Name"].str.strip().str.lower())
        new_names      = set(df["Full_Name"].str.strip().str.lower())
        joiners        = new_names - existing_names
        leavers        = existing_names - new_names
        if joiners:
            print(f"\n  ✨ New members  ({len(joiners)}): "
                  f"{', '.join(df[df['Full_Name'].str.lower().isin(joiners)]['Full_Name'])}")
        if leavers:
            print(f"  👋 Left club   ({len(leavers)}): "
                  f"{', '.join(existing[existing['Full_Name'].str.lower().isin(leavers)]['Full_Name'])}")
        if not joiners and not leavers:
            print("\n  No membership changes since last scrape.")
    except FileNotFoundError:
        print("  (First run — no previous data to compare)")

    CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CSV_FILE, index=False)

    print(f"\n✅ {len(df)} members saved → {CSV_FILE}")
    print(f"\n   Sample:")
    print(df[["Full_Name", "Membership", "Admin", "Owner"]].head(10).to_string(index=False))


# ── Entry point ────────────────────────────────────
if __name__ == "__main__":
    run()
