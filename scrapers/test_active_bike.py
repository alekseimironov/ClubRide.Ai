"""
scrapers/test_active_bike.py
Validates active bike detection from latest activity.
Reads 10 existing athlete IDs from athlete_profiles.csv and
scrapes their latest activity to find active bike.
Does NOT modify any existing CSV files.

Run: python scrapers/test_active_bike.py
"""

import json
import re
import time
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

ROOT         = Path(__file__).parent.parent
STRAVA_DIR   = ROOT.parent
COOKIES_FILE = STRAVA_DIR / "strava_cookies.json"
PROFILES_CSV = ROOT / "data/real/athlete_profiles.csv"


def load_cookies():
    with open(COOKIES_FILE) as f:
        return json.load(f)


def scrape_active_bike(page, athlete_id: str) -> str:
    """Navigate to latest activity and find which bike was used."""
    profile_url = f"https://www.strava.com/athletes/{athlete_id}"
    page.goto(profile_url, timeout=20000, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle", timeout=10000)
    time.sleep(1)

    # Find first activity link on profile
    activity_url = None
    try:
        links = page.query_selector_all("a[href*='/activities/']")
        for link in links:
            href = link.get_attribute("href") or ""
            if re.search(r"/activities/\d+$", href):
                activity_url = f"https://www.strava.com{href}" if href.startswith("/") else href
                break
    except Exception:
        pass

    if not activity_url:
        return "no activity found"

    # Navigate to the activity page
    try:
        page.goto(activity_url, timeout=20000, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=10000)
        time.sleep(1)

        body  = page.inner_text("body")
        lines = [l.strip() for l in body.split("\n") if l.strip()]

        for line in lines:
            m = re.match(r"^Bike:\s*(.+)", line, re.IGNORECASE)
            if m:
                return m.group(1).strip()

    except Exception as e:
        return f"error: {e}"

    return "not found"


def run():
    df      = pd.read_csv(PROFILES_CSV, dtype=str)
    sample  = df.head(10)
    cookies = load_cookies()

    print(f"\n{'='*60}")
    print(f"  ACTIVE BIKE VALIDATION — 10 athletes")
    print(f"  Reading IDs from: {PROFILES_CSV.name}")
    print(f"{'='*60}\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1440, "height": 900},
        )
        context.add_cookies(cookies)
        page = context.new_page()

        page.goto("https://www.strava.com/dashboard", timeout=30000)
        page.wait_for_load_state("networkidle")
        if "login" in page.url:
            print("❌ Session expired.")
            browser.close()
            return
        print("✅ Session valid\n")

        results = []
        for _, row in sample.iterrows():
            athlete_id = str(row["Athlete_ID"])
            name       = str(row.get("Name", ""))
            print(f"[{athlete_id}] {name}")

            active_bike = scrape_active_bike(page, athlete_id)
            print(f"  → Active bike: {active_bike}\n")
            results.append({"Athlete_ID": athlete_id, "Name": name,
                            "Active_Bike": active_bike})
            time.sleep(3)

        browser.close()

    print(f"\n{'─'*60}")
    print(f"  RESULTS SUMMARY")
    print(f"{'─'*60}")
    for r in results:
        print(f"  {r['Name'][:30]:30}  →  {r['Active_Bike']}")


if __name__ == "__main__":
    run()
