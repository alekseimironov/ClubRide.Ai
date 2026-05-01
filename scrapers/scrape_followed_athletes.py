"""
scrapers/scrape_followed_athletes.py
Scrapes Strava athlete profiles to build a real bike distribution dataset.

Agreed data format (one page load per athlete, no year-clicking):
  athlete_profiles.csv  — one row per athlete
  athlete_bikes.csv     — one row per bike (athlete can have 1-5 bikes)

Fields captured:
  Profile  : Athlete_ID, Name, Location
  All-Time : AllTime_km, AllTime_acts, AllTime_elev_m
  This Year: CurrYear_km, CurrYear_acts
  Best     : Longest_Ride_km, Biggest_Climb_m
  Bikes    : Name, km, extracted brand (in athlete_bikes.csv)

Security:
  - Random delay 4-9s between athletes
  - Max 40 athletes per session (resume on next run)
  - Skips private profiles automatically

Usage:
  python scrapers/scrape_followed_athletes.py --athlete 33177472
  python scrapers/scrape_followed_athletes.py --all
"""

import json
import re
import sys
import time
import random
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

ROOT         = Path(__file__).parent.parent
STRAVA_DIR   = ROOT.parent
COOKIES_FILE = STRAVA_DIR / "strava_cookies.json"
OUT_PROFILES = ROOT / "data/real/athlete_profiles.csv"
OUT_BIKES    = ROOT / "data/real/athlete_bikes.csv"

MAX_PER_SESSION = 250  # full collection — skips already-scraped IDs automatically

PROFILE_COLS = [
    "Athlete_ID", "Name", "Location",
    "AllTime_km", "AllTime_acts", "AllTime_elev_m", "AllTime_time_h",
    "CurrYear_km", "CurrYear_acts", "CurrYear_elev_m", "CurrYear_time_h",
    "Longest_Ride_km", "Biggest_Climb_m",
    "Bike_Count", "Active_Bike", "Multi_Sport", "Scraped_At",
]

BIKE_COLS = [
    "Athlete_ID", "Bike_Name", "Bike_Km", "Brand", "Scraped_At",
]

KNOWN_BRANDS = [
    "Trek", "Specialized", "Giant", "Scott", "Cannondale",
    "BMC", "Cervélo", "Cervelo", "Look", "Orbea", "Bianchi",
    "Pinarello", "Merida", "Canyon", "Colnago", "Wilier",
    "Cube", "Focus", "Felt", "Lapierre", "Time", "De Rosa",
    "Ridley", "Factor", "Rose", "Argon 18", "OPEN", "Specialized",
]


# ── Helpers ────────────────────────────────────────
def load_cookies():
    with open(COOKIES_FILE) as f:
        return json.load(f)

def parse_km(text: str) -> float:
    text = str(text).replace(",", "").replace(" ", "")
    m = re.search(r"([\d]+(?:\.\d+)?)", text)
    return float(m.group(1)) if m else 0.0

def extract_brand(bike_name: str) -> str:
    name_lower = bike_name.lower()
    for brand in KNOWN_BRANDS:
        if brand.lower() in name_lower:
            return brand
    return bike_name.split()[0] if bike_name.split() else "Unknown"

def already_scraped(athlete_id: str) -> bool:
    """Skip athlete if already in profiles CSV."""
    try:
        df = pd.read_csv(OUT_PROFILES, dtype=str)
        return athlete_id in df["Athlete_ID"].astype(str).values
    except FileNotFoundError:
        return False


# ── Page text parser ───────────────────────────────
def parse_profile_page(lines: list[str], athlete_id: str,
                        name: str, location: str) -> tuple[dict, list[dict]]:
    """
    Parse structured sections from the page text lines.
    Returns (profile_row, [bike_rows]).
    """
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # ── Section scanner ────────────────────────────
    alltime_km = alltime_acts = alltime_elev = alltime_time = 0.0
    curr_km    = curr_acts   = curr_elev   = curr_time    = 0.0
    longest    = biggest     = 0.0
    bikes      = []

    # ── Inline value extractor ─────────────────────
    def inline(text: str, label: str) -> float:
        rest = re.sub(re.escape(label), "", text, flags=re.IGNORECASE)
        m = re.search(r"([\d,]+\.?\d*)", rest)
        return parse_km(m.group(1)) if m else 0.0

    def parse_time_h(text: str) -> float:
        """Convert '1,588h 9m' or '101h 58m' → decimal hours."""
        text = text.replace(",", "")
        h = re.search(r"([\d]+)\s*h", text)
        m = re.search(r"([\d]+)\s*m", text)
        hours   = float(h.group(1)) if h else 0.0
        minutes = float(m.group(1)) if m else 0.0
        return round(hours + minutes / 60, 2)

    STOP_WORDS = (
        "shoes", "recent activit", "your recent", "share your",
        "refresh", "strava stories", "© 20", "about", "explore",
    )

    curr_year = str(datetime.now().year)
    section   = None   # "curr" | "alltime" | "best" | "bikes"

    for line in lines:
        ll = line.strip().lower()
        l  = line.strip()

        # ── Section transitions ────────────────────
        if re.match(rf"^{curr_year}\s*[▼▲]?$", l):
            section = "curr"
            continue
        if ll in ("all-time", "all time"):
            section = "alltime"
            continue
        if "best efforts" in ll:
            section = "best"
            continue
        if ll == "bikes":
            section = "bikes"
            continue
        # Stop section at known boundaries
        if section and any(kw in ll for kw in STOP_WORDS):
            section = None
            continue
        # Year change closes curr section
        if section == "curr" and ll in ("all-time", "all time"):
            section = "alltime"
            continue

        # ── Parse by section ───────────────────────
        if section == "curr":
            if "activities" in ll:
                curr_acts = inline(l, "Activities")
            elif "distance" in ll:
                curr_km   = inline(l, "Distance")
            elif "elev" in ll:
                curr_elev = inline(l, "Elev Gain")
            elif "time" in ll:
                curr_time = parse_time_h(l)

        elif section == "alltime":
            if "activities" in ll:
                alltime_acts = inline(l, "Activities")
            elif "distance" in ll:
                alltime_km   = inline(l, "Distance")
            elif "elev" in ll:
                alltime_elev = inline(l, "Elev Gain")
            elif "time" in ll:
                alltime_time = parse_time_h(l)

        elif section == "best":
            if "longest ride" in ll:
                longest = inline(l, "Longest Ride")
            elif "biggest climb" in ll:
                biggest = inline(l, "Biggest Climb")

        elif section == "bikes":
            km_m = re.search(r"([\d,]+\.?\d*)\s*km", l)
            if km_m:
                bike_name = l[:km_m.start()].strip().rstrip("—-–|").strip()
                bike_km   = parse_km(km_m.group(1))
                if bike_name and len(bike_name) > 1:
                    bikes.append((bike_name, bike_km))

    # ── Build profile row ──────────────────────────
    profile = {
        "Athlete_ID":      athlete_id,
        "Name":            name,
        "Location":        location,
        "AllTime_km":       alltime_km,
        "AllTime_acts":     int(alltime_acts),
        "AllTime_elev_m":   int(alltime_elev),
        "AllTime_time_h":   alltime_time,
        "CurrYear_km":      curr_km,
        "CurrYear_acts":    int(curr_acts),
        "CurrYear_elev_m":  int(curr_elev),
        "CurrYear_time_h":  curr_time,
        "Longest_Ride_km": longest,
        "Biggest_Climb_m": biggest,
        "Bike_Count":      len(bikes),
        "Scraped_At":      now,
    }

    bike_rows = [
        {
            "Athlete_ID": athlete_id,
            "Bike_Name":  name_b,
            "Bike_Km":    km_b,
            "Brand":      extract_brand(name_b),
            "Scraped_At": now,
        }
        for name_b, km_b in bikes
    ]

    return profile, bike_rows


# ── Find active bike from latest activity ──────────
def scrape_active_bike(page, profile_lines: list[str], athlete_id: str) -> str:
    """
    Navigate to the athlete's latest activity and extract which bike was used.
    Strava shows gear on the activity page — this is the definitive signal
    for which bike they are currently riding.
    Returns bike name string or "" if not found.
    """
    # Find first activity link from the profile page lines
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
        return ""

    try:
        if not goto_with_retry(page, activity_url, retries=2):
            return ""
        page.wait_for_load_state("networkidle", timeout=10000)
        time.sleep(1)

        body = page.inner_text("body")
        lines = [l.strip() for l in body.split("\n") if l.strip()]

        # Activity page format: "Bike: Canyon Endurace CF SL"
        for line in lines:
            m = re.match(r"^Bike:\s*(.+)", line, re.IGNORECASE)
            if m:
                return m.group(1).strip()

        # Navigate back to profile for next operations
        page.goto(f"https://www.strava.com/athletes/{athlete_id}",
                  timeout=20000, wait_until="domcontentloaded")
        page.wait_for_load_state("networkidle", timeout=10000)
        time.sleep(1)

    except Exception as e:
        print(f"  ⚠️  Active bike lookup failed: {e}")

    return ""


# ── Retry-safe page navigation ────────────────────
def goto_with_retry(page, url: str, retries: int = 3) -> bool:
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, timeout=45000, wait_until="domcontentloaded")
            return True
        except Exception:
            wait = 20 * attempt   # 20s, 40s, 60s
            print(f"  Timeout attempt {attempt}/{retries} — waiting {wait}s")
            if attempt < retries:
                time.sleep(wait)
    return False


# ── Scrape one athlete ─────────────────────────────
def scrape_athlete(page, athlete_id: str) -> tuple[dict | None, list[dict]]:
    url = f"https://www.strava.com/athletes/{athlete_id}"

    try:
        ok = goto_with_retry(page, url)
        if not ok:
            print(f"  Failed after retries — skipping")
            return None, []
        page.wait_for_load_state("networkidle", timeout=15000)
        time.sleep(1.5)

        if page.url and "404" in page.title():
            print(f"  ⚠️  Not found (404)")
            return None, []

        # Private check
        quick = page.inner_text("body")
        if "this account is private" in quick.lower():
            print(f"  🔒 Private — skipping")
            return None, []

        # Scroll to load all sections (needed before tab detection)
        for _ in range(6):
            page.evaluate("window.scrollBy(0, 600)")
            time.sleep(0.4)
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(0.5)

        body_text = page.inner_text("body")
        lines     = [l.strip() for l in body_text.split("\n") if l.strip()]

        # ── Name + primary sport from page title ───────
        name          = ""
        primary_sport = "cyclist"
        multi_sport   = False
        try:
            title = page.title()
            sport_m = re.search(
                r"(Cyclist|Runner|Swimmer|Triathlete|Hiker|Walker|Skier)\s+Profile",
                title, re.IGNORECASE
            )
            if sport_m:
                primary_sport = sport_m.group(1).lower()
            name = re.sub(
                r"\s*(Cyclist|Runner|Swimmer|Triathlete|Hiker|Walker|Skier)\s+Profile\s*",
                "", title, flags=re.IGNORECASE
            )
            name = name.replace("| Strava", "").strip()
            name = re.sub(r"\s{2,}", " ", name).strip()
        except Exception:
            pass

        # ── Multi-sport: try clicking cycling tab ───────
        if primary_sport != "cyclist":
            cycling_tab = (
                page.query_selector("li.sport.cycling a") or
                page.query_selector("li.sport.cycling")
            )
            if cycling_tab:
                print(f"  Multi-sport ({primary_sport}) — clicking cycling tab")
                cycling_tab.click()
                page.wait_for_load_state("networkidle", timeout=15000)
                time.sleep(2)
                # Re-scroll and re-read after tab switch
                for _ in range(6):
                    page.evaluate("window.scrollBy(0, 600)")
                    time.sleep(0.4)
                page.evaluate("window.scrollTo(0, 0)")
                time.sleep(0.5)
                body_text = page.inner_text("body")
                lines     = [l.strip() for l in body_text.split("\n") if l.strip()]
                multi_sport = True
            else:
                print(f"  Primary sport: '{primary_sport}', no cycling tab — skipping")
                return None, []

        # ── Location — scan lines after name for City, Country pattern ──
        location = ""
        try:
            for i, line in enumerate(lines):
                if line.strip() == name.strip() and i + 1 < len(lines):
                    # Check next 4 lines for "City, Country" pattern
                    for candidate in lines[i + 1:i + 5]:
                        candidate = candidate.strip()
                        if ("," in candidate
                                and len(candidate) < 60
                                and "club" not in candidate.lower()
                                and "subscriber" not in candidate.lower()):
                            # Normalise "Lausanne , Switzerland" → "Lausanne, Switzerland"
                            location = re.sub(r"\s*,\s*", ", ", candidate).strip()
                            break
                    break
        except Exception:
            pass

        # ── Scrape latest activity to find active bike ─
        active_bike = scrape_active_bike(page, lines, athlete_id)
        if active_bike:
            print(f"  🚲 Active bike (latest activity): {active_bike}")

        profile, bike_rows = parse_profile_page(lines, athlete_id, name, location)
        profile["Active_Bike"]  = active_bike or ""
        profile["Multi_Sport"]  = multi_sport

        # ── Print result ───────────────────────────
        print(f"  Name       : {name}")
        print(f"  Location   : {location or '—'}")
        print(f"  All-Time   : {profile['AllTime_km']:,.0f} km  "
              f"{profile['AllTime_acts']} acts  "
              f"{profile['AllTime_elev_m']:,.0f} m elev  "
              f"{profile['AllTime_time_h']:,.1f} h")
        print(f"  {datetime.now().year}       : {profile['CurrYear_km']:,.0f} km  "
              f"{profile['CurrYear_acts']} acts  "
              f"{profile['CurrYear_elev_m']:,.0f} m elev  "
              f"{profile['CurrYear_time_h']:,.1f} h")
        print(f"  Best       : Longest {profile['Longest_Ride_km']:.0f} km  "
              f"Climb {profile['Biggest_Climb_m']:.0f} m")
        print(f"  Bikes ({profile['Bike_Count']})  :", end="")
        if bike_rows:
            print()
            for b in bike_rows:
                print(f"    → {b['Bike_Name']:30}  {b['Bike_Km']:>8,.0f} km  "
                      f"[{b['Brand']}]")
        else:
            print(" none found (private or no gear listed)")

        return profile, bike_rows

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return None, []


# ── Save results ───────────────────────────────────
def save(profiles: list[dict], bikes: list[dict]):
    if profiles:
        new_p = pd.DataFrame(profiles, columns=PROFILE_COLS)
        try:
            existing = pd.read_csv(OUT_PROFILES, dtype=str)
            known    = set(existing["Athlete_ID"].astype(str))
            fresh    = new_p[~new_p["Athlete_ID"].astype(str).isin(known)]
            final    = pd.concat([existing, fresh], ignore_index=True)
        except FileNotFoundError:
            final = new_p
        OUT_PROFILES.parent.mkdir(parents=True, exist_ok=True)
        final.to_csv(OUT_PROFILES, index=False)
        print(f"\n  Profiles saved : {len(profiles)} new → {OUT_PROFILES.name}")
        print(f"  Total profiles : {len(final)}")

    if bikes:
        new_b = pd.DataFrame(bikes, columns=BIKE_COLS)
        try:
            existing = pd.read_csv(OUT_BIKES, dtype=str)
            final_b  = pd.concat([existing, new_b], ignore_index=True)
        except FileNotFoundError:
            final_b = new_b
        OUT_BIKES.parent.mkdir(parents=True, exist_ok=True)
        final_b.to_csv(OUT_BIKES, index=False)
        print(f"  Bikes saved    : {len(bikes)} rows → {OUT_BIKES.name}")


# ── Run scraping session ───────────────────────────
def run(athlete_ids: list[str]):
    cookies   = load_cookies()
    profiles  = []
    bikes     = []
    scraped   = 0
    skipped   = 0

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

        for i, athlete_id in enumerate(athlete_ids, 1):
            if already_scraped(athlete_id):
                print(f"[{i:03}/{len(athlete_ids)}] {athlete_id} — already scraped, skipping")
                skipped += 1
                continue

            if scraped >= MAX_PER_SESSION:
                print(f"\n⏸️  Session limit ({MAX_PER_SESSION}) reached — run again to continue")
                break

            print(f"[{i:03}/{len(athlete_ids)}] Athlete {athlete_id}")
            profile, bike_rows = scrape_athlete(page, athlete_id)

            if profile:
                profiles.append(profile)
                bikes.extend(bike_rows)
                scraped += 1

            # Save every 10 athletes — survive crashes
            if len(profiles) > 0 and len(profiles) % 10 == 0:
                save(profiles, bikes)
                profiles, bikes = [], []

            # Delay between athletes
            if i < len(athlete_ids):
                # Cooldown every 20 scraped athletes
                if scraped > 0 and scraped % 20 == 0:
                    print(f"  Cooling down 90s after {scraped} athletes...")
                    time.sleep(90)
                else:
                    delay = random.uniform(10, 18)
                    print(f"  waiting {delay:.1f}s\n")
                    time.sleep(delay)

        browser.close()

    # Save any remaining
    if profiles:
        save(profiles, bikes)
    print(f"\n{'='*50}")
    print(f"  Session complete: {scraped} scraped, {skipped} skipped")
    print(f"{'='*50}")


# ── Fetch following list ───────────────────────────
def fetch_following_ids(self_id: str) -> list[str]:
    cookies = load_cookies()
    ids     = []

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

        # Strava paginates following list at 25 per page via ?page=N
        base_url = f"https://www.strava.com/athletes/{self_id}/follows?type=following"
        page_num = 1

        while True:
            url_p = f"{base_url}&page={page_num}"
            print(f"  Page {page_num}: {url_p}")
            page.goto(url_p, timeout=30000, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle")
            time.sleep(2)

            links = page.query_selector_all("a[href*='/athletes/']")
            page_ids = set()
            for link in links:
                href = link.get_attribute("href") or ""
                m    = re.search(r"/athletes/(\d+)", href)
                if m and m.group(1) != self_id:
                    page_ids.add(m.group(1))

            # Filter out IDs already collected
            new_ids = page_ids - set(ids)
            print(f"  → {len(page_ids)} athletes on page, {len(new_ids)} new")

            if not new_ids:
                print(f"  No new athletes on page {page_num} — done")
                break

            ids.extend(new_ids)
            page_num += 1
            time.sleep(1)

        links = page.query_selector_all("a[href*='/athletes/']")
        for link in links:
            href = link.get_attribute("href") or ""
            m    = re.search(r"/athletes/(\d+)", href)
            if m and m.group(1) != self_id:
                ids.append(m.group(1))

        ids = list(dict.fromkeys(ids))
        browser.close()

    print(f"\n  Found {len(ids)} followed athletes")
    return ids


# ── Entry point ────────────────────────────────────
if __name__ == "__main__":
    if "--athlete" in sys.argv:
        idx        = sys.argv.index("--athlete")
        athlete_id = sys.argv[idx + 1]
        print(f"\n{'='*50}")
        print(f"  Scraping athlete {athlete_id}")
        print(f"{'='*50}\n")
        run([athlete_id])

    elif "--all" in sys.argv:
        self_id = "33177472"
        if "--self-id" in sys.argv:
            idx     = sys.argv.index("--self-id")
            self_id = sys.argv[idx + 1]
        ids = fetch_following_ids(self_id)
        if ids:
            run(ids)
    else:
        print("Usage:")
        print("  python scrapers/scrape_followed_athletes.py --athlete 33177472")
        print("  python scrapers/scrape_followed_athletes.py --all")
