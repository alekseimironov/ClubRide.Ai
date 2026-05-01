"""
scrapers/scrape_active_bikes.py
Scrapes the active bike from the latest activity for all athletes
who have at least one bike in athlete_bikes.csv.

Visits each athlete's profile → clicks latest activity → reads "Bike: ..."
Saves to data/real/active_bikes.csv (separate file, no existing data touched).
Resumes automatically — skips already-scraped athlete IDs.

Run: python scrapers/scrape_active_bikes.py
"""

import json
import re
import time
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

ROOT         = Path(__file__).parent.parent
STRAVA_DIR   = ROOT.parent
COOKIES_FILE = STRAVA_DIR / "strava_cookies.json"
PROFILES_CSV = ROOT / "data/real/athlete_profiles.csv"
BIKES_CSV    = ROOT / "data/real/athlete_bikes.csv"
OUT_CSV      = ROOT / "data/real/active_bikes.csv"

COLUMNS = ["Athlete_ID", "Name", "Active_Bike", "Activity_URL", "Scraped_At"]


def load_cookies():
    with open(COOKIES_FILE) as f:
        return json.load(f)


def already_done() -> set:
    try:
        df = pd.read_csv(OUT_CSV, dtype=str)
        return set(df["Athlete_ID"].astype(str))
    except FileNotFoundError:
        return set()


def goto_with_retry(page, url: str, retries: int = 3) -> bool:
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, timeout=45000, wait_until="load")
            return True
        except Exception:
            wait = 15 * attempt   # 15s, 30s, 45s — gives Strava time to recover
            print(f"  ⚠️  Attempt {attempt}/{retries} failed — waiting {wait}s")
            if attempt < retries:
                time.sleep(wait)
    return False


def get_active_bike(page, athlete_id: str) -> tuple[str, str]:
    """
    Returns (bike_name, activity_url) from the athlete's latest activity.
    """
    if not goto_with_retry(page, f"https://www.strava.com/athletes/{athlete_id}"):
        return "", ""
    time.sleep(1.5)

    # Find first activity link
    activity_url = ""
    try:
        links = page.query_selector_all("a[href*='/activities/']")
        for link in links:
            href = link.get_attribute("href") or ""
            if re.search(r"/activities/\d+$", href):
                activity_url = (f"https://www.strava.com{href}"
                                if href.startswith("/") else href)
                break
    except Exception:
        pass

    if not activity_url:
        return "", ""

    # Load activity page
    try:
        if not goto_with_retry(page, activity_url):
            return "", activity_url
        time.sleep(1)

        body  = page.inner_text("body")
        lines = [l.strip() for l in body.split("\n") if l.strip()]

        for line in lines:
            m = re.match(r"^Bike:\s*(.+)", line, re.IGNORECASE)
            if m:
                return m.group(1).strip(), activity_url

    except Exception as e:
        print(f"  ⚠️  Activity page error: {e}")

    return "", activity_url


def save_batch(rows: list[dict]):
    new_df = pd.DataFrame(rows, columns=COLUMNS)
    try:
        existing = pd.read_csv(OUT_CSV, dtype=str)
        final    = pd.concat([existing, new_df], ignore_index=True)
    except FileNotFoundError:
        final = new_df
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(OUT_CSV, index=False)


def run():
    print(f"\n{'='*60}")
    print(f"  🚲 ACTIVE BIKE SCRAPER")
    print(f"{'='*60}\n")

    # Load athletes with bikes only
    profiles = pd.read_csv(PROFILES_CSV, dtype=str)
    bikes    = pd.read_csv(BIKES_CSV,    dtype=str)

    athletes_with_bikes = set(bikes["Athlete_ID"].astype(str))
    profiles["Bike_Count"] = pd.to_numeric(profiles["Bike_Count"], errors="coerce").fillna(0)
    target = profiles[profiles["Athlete_ID"].astype(str).isin(athletes_with_bikes)].copy()

    done    = already_done()
    pending = target[~target["Athlete_ID"].astype(str).isin(done)]

    print(f"  Athletes with bikes  : {len(target)}")
    print(f"  Already scraped      : {len(done)}")
    print(f"  Remaining            : {len(pending)}\n")

    if pending.empty:
        print("  ✅ All done — nothing to scrape.")
        return

    cookies  = load_cookies()
    rows     = []
    scraped  = 0
    no_bike  = 0

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
            print("❌ Session expired — run save_cookies.py first.")
            browser.close()
            return
        print("✅ Session valid\n")

        total = len(pending)
        for i, (_, row) in enumerate(pending.iterrows(), 1):
            athlete_id = str(row["Athlete_ID"])
            name       = str(row.get("Name", ""))

            print(f"[{i:03}/{total}] {name[:35]}", end="  ")

            bike, act_url = get_active_bike(page, athlete_id)

            if bike:
                print(f"→ {bike}")
                scraped += 1
            else:
                print(f"→ no bike found (private activity or no gear set)")
                no_bike += 1

            rows.append({
                "Athlete_ID":   athlete_id,
                "Name":         name,
                "Active_Bike":  bike,
                "Activity_URL": act_url,
                "Scraped_At":   datetime.now().strftime("%Y-%m-%d %H:%M"),
            })

            # Save every 10 athletes (resume safety)
            if len(rows) % 10 == 0:
                save_batch(rows)
                rows = []

            if i < total:
                delay = random.uniform(10, 18)
                # Extra pause every 20 athletes to avoid session throttling
                if i % 20 == 0:
                    print(f"  ⏸️  Cooling down 60s after {i} athletes...")
                    time.sleep(60)
                else:
                    time.sleep(delay)

        browser.close()

    # Save remaining
    if rows:
        save_batch(rows)

    print(f"\n{'='*60}")
    print(f"  Active bikes found : {scraped}")
    print(f"  No bike recorded   : {no_bike}")
    print(f"  Saved → {OUT_CSV.name}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
