"""
scrapers/scrape_leaderboard.py
Scrapes TNCE leaderboard daily at 22:00.

Strategy: daily snapshots of "This Week" top-100 + "Last Week" on Sundays.
By taking the UNION across all daily snapshots of a week, we capture
athletes who were in top-100 on any day — yielding 150-200+ unique
athletes vs just 100 from a single Sunday scrape.

Data model:
  Each row = one athlete's cumulative stats at one daily snapshot.
  Key: (Year, Week_Number, Athlete, Snapshot_Date) — no duplicates per day.
  For weekly reporting: take MAX(Distance_km) per athlete per week
  to get their best/final stats.

Output: data/real/leaderboard.csv
"""

import json
import re
import sys
import time
from datetime import datetime, date, timedelta
from pathlib import Path

import pandas as pd
import schedule
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# ── Paths ──────────────────────────────────────────
ROOT         = Path(__file__).parent.parent
STRAVA_DIR   = ROOT.parent
CSV_FILE     = ROOT / "data/real/leaderboard.csv"
COOKIES_FILE = STRAVA_DIR / "strava_cookies.json"
CONFIG_FILE  = ROOT / "config.json"

load_dotenv(ROOT / ".env")


# ── Helpers ────────────────────────────────────────
def load_cookies():
    with open(COOKIES_FILE) as f:
        return json.load(f)

def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)

def week_info(offset=0):
    d = date.today() - timedelta(weeks=offset)
    iso = d.isocalendar()
    return iso[0], iso[1]

def is_sunday():
    return date.today().weekday() == 6


# ── Leaderboard parser ─────────────────────────────
def parse_table(page, year, week, week_label, snapshot_date):
    rows = []
    try:
        page.wait_for_selector("table.dense.striped.sortable tbody tr", timeout=10000)
        time.sleep(1)

        for tr in page.query_selector_all("table.dense.striped.sortable tbody tr"):

            def cell(selector):
                el = tr.query_selector(selector)
                return re.sub(r"\s+", " ", el.inner_text()).strip() if el else ""

            athlete_el = tr.query_selector(
                "td.athlete a.athlete-name, td.athlete a[class*='athlete-name']"
            )
            athlete = (
                re.sub(r"\s+", " ", athlete_el.inner_text()).strip()
                if athlete_el else cell("td.athlete")
            )
            if not athlete:
                continue

            long_el  = tr.query_selector("td.longest-activity a.minimal, td.longest-activity a")
            raw_long = (
                re.sub(r"\s+", " ", long_el.inner_text()).strip()
                if long_el else cell("td.longest-activity")
            )

            def parse_num(text):
                text = text.replace(",", "")
                m = re.search(r"[\d]+(?:\.\d+)?", text)
                return float(m.group()) if m else None

            rows.append({
                "Snapshot_Date": snapshot_date,
                "Year":          year,
                "Week_Number":   week,
                "Week_Label":    week_label,
                "Rank":          cell("td.rank"),
                "Athlete":       athlete,
                "Distance_km":   parse_num(cell("td[class*='distance']")),
                "Rides":         cell("td.num-activities"),
                "Longest_km":    parse_num(raw_long),
                "Avg_Speed_kmh": parse_num(cell("td.average-speed")),
                "Elev_Gain_m":   parse_num(cell("td.elev-gain")),
                "Scraped_At":    datetime.now().strftime("%Y-%m-%d %H:%M"),
            })

    except Exception as e:
        print(f"    Table parse error: {e}")
    return rows


# ── Reliable navigation with retry ────────────────
def goto_with_retry(page, url, retries=3, timeout=45000):
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle", timeout=15000)
            return True
        except Exception as e:
            print(f"    Navigation attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(3 * attempt)
    return False


# ── Main scrape ────────────────────────────────────
def scrape():
    print(f"\n{'='*60}")
    print(f" Leaderboard snapshot  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    cfg           = load_config()
    club_id       = cfg["club"]["club_id"]
    cookies       = load_cookies()
    snapshot_date = date.today().isoformat()
    all_rows      = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        context.add_cookies(cookies)
        page = context.new_page()

        if not goto_with_retry(page, "https://www.strava.com/dashboard"):
            print(" Could not reach Strava after 3 attempts  check network.")
            browser.close()
            return
        if "login" in page.url:
            print(" Session expired  run save_cookies.py in the Strava folder.")
            browser.close()
            return
        print(" Session valid")

        if not goto_with_retry(page, f"https://www.strava.com/clubs/{club_id}/leaderboard"):
            print(" Could not reach leaderboard page.")
            browser.close()
            return
        time.sleep(2)

        # ── This Week (every day) ──────────────────
        year_tw, week_tw = week_info(offset=0)
        print(f"\n This Week  W{week_tw} {year_tw}  [snapshot {snapshot_date}]")
        btn = page.query_selector(
            "span.button.this-week, a.button.this-week, li.this-week span.button"
        )
        if btn:
            btn.click()
            time.sleep(2)
        rows = parse_table(page, year_tw, week_tw, "This Week", snapshot_date)
        print(f"   {len(rows)} athletes")
        all_rows.extend(rows)

        # ── Last Week (Sundays only — final snapshot) ──
        if is_sunday():
            year_lw, week_lw = week_info(offset=1)
            print(f"\n Last Week  W{week_lw} {year_lw}  [Sunday final snapshot]")
            btn = page.query_selector(
                "span.button.last-week, a.button.last-week, li.last-week span.button"
            )
            if btn:
                btn.click()
                time.sleep(2)
            rows = parse_table(page, year_lw, week_lw, "Last Week", snapshot_date)
            print(f"   {len(rows)} athletes")
            all_rows.extend(rows)

        browser.close()

    if not all_rows:
        print(" No data scraped.")
        return

    new_df = pd.DataFrame(all_rows)

    # Upsert — dedup by (Snapshot_Date, Year, Week_Number, Athlete)
    # Keeps all daily snapshots, replaces if re-run on same day
    try:
        existing = pd.read_csv(CSV_FILE, dtype=str)

        # Migrate legacy CSV that has no Snapshot_Date column
        if "Snapshot_Date" not in existing.columns:
            existing["Snapshot_Date"] = existing.get("Scraped_At", "").str[:10]
            print("     Legacy CSV migrated  Snapshot_Date added from Scraped_At")

        # Drop rows matching today's snapshot (re-run safety)
        mask = (
            (existing["Snapshot_Date"] == snapshot_date) &
            (existing["Year"].str.split(".").str[0] == str(year_tw)) &
            (existing["Week_Number"].str.split(".").str[0] == str(week_tw))
        )
        existing = existing[~mask]
        final    = pd.concat([existing, new_df], ignore_index=True)
        n_prev_snapshots = existing["Snapshot_Date"].nunique()
    except FileNotFoundError:
        final            = new_df
        n_prev_snapshots = 0

    final = (
        final
        .assign(
            Year=lambda d: pd.to_numeric(d["Year"], errors="coerce").astype("Int64"),
            Week_Number=lambda d: pd.to_numeric(d["Week_Number"], errors="coerce").astype("Int64"),
            Rank=lambda d: pd.to_numeric(d["Rank"], errors="coerce").astype("Int64"),
        )
        .sort_values(["Year", "Week_Number", "Snapshot_Date", "Rank"])
        .reset_index(drop=True)
    )

    CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
    final.to_csv(CSV_FILE, index=False)

    # Weekly unique athletes (union across daily snapshots this week)
    this_week = final[
        (final["Year"].astype(str).str.split(".").str[0] == str(year_tw)) &
        (final["Week_Number"].astype(str).str.split(".").str[0] == str(week_tw))
    ]
    unique_athletes = this_week["Athlete"].nunique()
    snapshots_taken = this_week["Snapshot_Date"].nunique()

    print(f"\n Snapshot saved  {CSV_FILE}")
    print(f"   W{week_tw}/{year_tw}: {snapshots_taken} daily snapshot(s)  "
          f"{unique_athletes} unique athletes captured so far this week")
    print(f"   Total snapshots in CSV: {n_prev_snapshots + 1}")
    print(f"\n   Today's top 10:")
    today = new_df[new_df["Week_Label"] == "This Week"].head(10)
    print(today[["Rank", "Athlete", "Distance_km", "Rides"]].to_string(index=False))


# ── Entry point ────────────────────────────────────
if __name__ == "__main__":
    if "--now" in sys.argv:
        scrape()
    else:
        print("  Scheduler active  runs daily at 22:00")
        print("   Tip: python scrapers/scrape_leaderboard.py --now")
        schedule.every().day.at("22:00").do(scrape)
        while True:
            schedule.run_pending()
            time.sleep(30)
