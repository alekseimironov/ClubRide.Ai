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
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

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
        "Athlete_ID":  m.get("id", ""),
        "Full_Name":   full_name,
        "Firstname":   firstname,
        "Lastname":    lastname,
        "Membership":  m.get("membership", "member"),
        "Admin":       m.get("admin", False),
        "Owner":       m.get("owner", False),
        "Scraped_At":  datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


# ── Scrape club members page for full names + IDs ──
def scrape_club_member_ids(club_id: int) -> dict:
    """
    Scrapes https://www.strava.com/clubs/{id}/members via Playwright.
    Returns {athlete_id: full_name} — full names (not API-truncated).
    Paginates until no new IDs found.
    """
    cookies_path = ROOT.parent / "strava_cookies.json"
    with open(cookies_path) as f:
        cookies = json.load(f)

    id_to_name = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        context.add_cookies(cookies)
        page = context.new_page()

        page.goto("https://www.strava.com/dashboard", timeout=30000,
                  wait_until="domcontentloaded")
        if "login" in page.url:
            print("  Session expired — run save_cookies.py first")
            browser.close()
            return id_to_name

        page_num = 1
        while True:
            url = f"https://www.strava.com/clubs/{club_id}/members?page={page_num}"
            page.goto(url, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle", timeout=15000)
            time.sleep(1.5)

            links = page.query_selector_all("a[href*='/athletes/']")
            new_found = 0
            for link in links:
                href = link.get_attribute("href") or ""
                m    = re.search(r"/athletes/(\d+)", href)
                if not m:
                    continue
                aid  = m.group(1)
                if aid in id_to_name:
                    continue
                # Get name from link text or nearby element
                name = (link.inner_text() or "").strip()
                name = re.sub(r"\s+", " ", name).strip()
                if name:
                    id_to_name[aid] = name
                    new_found += 1

            print(f"  Page {page_num}: {new_found} new members (total {len(id_to_name)})")
            if new_found == 0:
                break
            page_num += 1
            time.sleep(1)

        browser.close()

    print(f"  Club members page: {len(id_to_name)} IDs found")
    return id_to_name


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

    parsed = [p for p in [parse_member(m) for m in raw_members] if p is not None]
    df = pd.DataFrame(parsed)

    # Enrich with IDs from club members page (API truncates names + omits IDs)
    print(f"\n🔍 Scraping club members page for full names + IDs...")
    id_map = scrape_club_member_ids(club_id)  # {id: full_name}

    if id_map:
        # Build reverse lookup: norm(full_name) → id
        import sys as _sys
        if str(ROOT) not in _sys.path:
            _sys.path.insert(0, str(ROOT))
        from brain.retriever import _norm
        name_to_id = {_norm(name): aid for aid, name in id_map.items()}

        # Match API rows (abbreviated names) to page IDs by first name + initial
        matched = 0
        for i, row in df.iterrows():
            if row.get("Athlete_ID") not in ("", None) and str(row.get("Athlete_ID")) != "nan":
                continue  # already has ID
            first = str(row.get("Firstname", "")).strip().lower()
            last  = str(row.get("Lastname", "")).strip().lower()
            last_initial = last[0] if last else ""
            # Try exact norm match first
            full_norm = _norm(f"{first} {last}")
            if full_norm in name_to_id:
                df.at[i, "Athlete_ID"] = name_to_id[full_norm]
                matched += 1
                continue
            # Try first name + last initial match
            for norm_name, aid in name_to_id.items():
                parts = norm_name.split()
                if (len(parts) >= 2 and parts[0] == first
                        and parts[1][0] == last_initial):
                    df.at[i, "Athlete_ID"] = aid
                    matched += 1
                    break

        print(f"  Matched {matched} IDs from members page")
        # Also store full names from page where available
        for aid, full_name in id_map.items():
            mask = df["Athlete_ID"].astype(str) == str(aid)
            if mask.any():
                df.loc[mask, "Full_Name"] = full_name

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
