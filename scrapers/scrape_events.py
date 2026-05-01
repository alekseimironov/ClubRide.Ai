"""
scrapers/scrape_events.py
Scrapes TNCE club events and attendance from Strava.
Output: data/real/historical_attendance.csv

Adapted from scrape_historical_TNCE.py — path and config changes only:
  - Credentials from .env (STRAVA_CLIENT_ID / STRAVA_CLIENT_SECRET)
  - Club settings from config.json (club_id, start_date, fallback coords)
  - Cookies/token from ../strava_cookies.json and ../strava_token.json
  - Output written to data/real/
"""

import json
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

# ── Paths ──────────────────────────────────────────
ROOT         = Path(__file__).parent.parent          # ClubRide.Ai/
STRAVA_DIR   = ROOT.parent                           # Strava/
CSV_FILE     = ROOT / "data/real/historical_attendance.csv"
COOKIES_FILE = STRAVA_DIR / "strava_cookies.json"
TOKEN_FILE   = STRAVA_DIR / "strava_token.json"
CONFIG_FILE  = ROOT / "config.json"

REFRESH_WINDOW_DAYS = 28

load_dotenv(ROOT / ".env")


# ── Config ─────────────────────────────────────────
def load_config():
    with open(CONFIG_FILE) as f:
        raw = json.load(f)
    club = raw["club"]
    return {
        "client_id":     os.getenv("STRAVA_CLIENT_ID"),
        "client_secret": os.getenv("STRAVA_CLIENT_SECRET"),
        "club_id":       club["club_id"],
        "start_date":    club["start_date"],
        "fallback_lat":  club["fallback_lat"],
        "fallback_lon":  club["fallback_lon"],
    }

def load_cookies():
    with open(COOKIES_FILE) as f:
        return json.load(f)

def load_token():
    with open(TOKEN_FILE) as f:
        return json.load(f)

def save_token(data):
    with open(TOKEN_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ── Token management ───────────────────────────────
def get_valid_token(config):
    token_data = load_token()
    if time.time() > token_data.get("expires_at", 0) - 60:
        print("🔄 Refreshing access token...")
        r = requests.post("https://www.strava.com/oauth/token", data={
            "client_id":     config["client_id"],
            "client_secret": config["client_secret"],
            "grant_type":    "refresh_token",
            "refresh_token": token_data["refresh_token"],
        })
        token_data = r.json()
        save_token(token_data)
        print("✅ Token refreshed.")
    return token_data["access_token"]


# ── Load existing CSV ──────────────────────────────
def load_existing_data():
    try:
        df = pd.read_csv(CSV_FILE, dtype=str)
        print(f"📂 Loaded existing CSV — {len(df)} events")
        return df, set(df["Event_ID"].tolist())
    except FileNotFoundError:
        print("📂 No existing CSV — creating fresh")
        return pd.DataFrame(), set()


# ── Event action ───────────────────────────────────
def get_event_action(event, existing_ids):
    eid        = str(event["id"])
    event_date = datetime.strptime(event["date"], "%Y-%m-%d")
    cutoff     = datetime.now() - timedelta(days=REFRESH_WINDOW_DAYS)
    if eid in existing_ids:
        return "refresh" if event_date >= cutoff else "skip"
    return "new"


# ── Fetch club events via API ──────────────────────
def fetch_club_events(access_token, config):
    print("📡 Fetching club events from Strava API...")
    headers    = {"Authorization": f"Bearer {access_token}"}
    r          = requests.get(
        f"https://www.strava.com/api/v3/clubs/{config['club_id']}/group_events",
        headers=headers,
    )
    all_events = r.json()
    print(f"  API returned {len(all_events)} events")

    start_date = datetime.strptime(config["start_date"], "%Y-%m-%d")
    filtered   = []
    for e in all_events:
        try:
            if e.get("upcoming_occurrences"):
                event_date = datetime.strptime(e["upcoming_occurrences"][0][:10], "%Y-%m-%d")
            else:
                event_date = datetime.strptime(e["start_date"][:10], "%Y-%m-%d")
            if event_date >= start_date:
                filtered.append({"id": e["id"], "title": e.get("title", "Unknown"),
                                  "date": event_date.strftime("%Y-%m-%d")})
        except Exception:
            pass

    seen, deduped = set(), []
    for e in filtered:
        eid = str(e["id"])
        if eid not in seen:
            seen.add(eid)
            deduped.append(e)

    print(f"  Found {len(deduped)} unique events since {config['start_date']}")
    return deduped


# ── Polyline decoder ───────────────────────────────
def decode_polyline(polyline_str):
    index, lat, lng = 0, 0, 0
    coordinates     = []
    changes         = {"latitude": 0, "longitude": 0}
    while index < len(polyline_str):
        for unit in ["latitude", "longitude"]:
            shift, result = 0, 0
            while True:
                byte = ord(polyline_str[index]) - 63
                index += 1
                result |= (byte & 0x1F) << shift
                shift  += 5
                if not byte >= 0x20:
                    break
            changes[unit] = ~(result >> 1) if result & 1 else result >> 1
        lat += changes["latitude"]
        lng += changes["longitude"]
        coordinates.append((lat / 1e5, lng / 1e5))
    return coordinates


# ── Route details ──────────────────────────────────
def get_route_details(route_id, access_token, config):
    r    = requests.get(
        f"https://www.strava.com/api/v3/routes/{route_id}",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    data = r.json()
    lat, lon = config["fallback_lat"], config["fallback_lon"]
    sub_type_map = {1: "Road", 2: "Mountain", 3: "Cross", 4: "Trail", 5: "Mixed"}
    route_type   = sub_type_map.get(data.get("sub_type"), "Unknown")

    if data.get("map") and data["map"].get("summary_polyline"):
        coords = decode_polyline(data["map"]["summary_polyline"])
        if coords:
            mid      = len(coords) // 2
            lat, lon = coords[mid]
    elif data.get("start_latlng"):
        lat, lon = data["start_latlng"]

    return {
        "lat": lat, "lon": lon,
        "route_name":            data.get("name", "Unknown"),
        "route_type":            route_type,
        "estimated_moving_time": data.get("estimated_moving_time", 14400),
        "weather_location":      "route_midpoint",
    }


# ── Weather ────────────────────────────────────────
def weather_description(code):
    if code is None:          return "Unknown"
    if code == 0:             return "Clear sky"
    if code in [1, 2, 3]:    return "Partly cloudy"
    if code in [45, 48]:     return "Foggy"
    if code in [51, 53, 55]: return "Drizzle"
    if code in [61, 63, 65]: return "Rain"
    if code in [71, 73, 75]: return "Snow"
    if code in [80, 81, 82]: return "Rain showers"
    if code in [95, 96, 99]: return "Thunderstorm"
    return "Other"


def get_weather(lat, lon, date_str, start_time_str, duration_seconds):
    try:
        start_dt   = datetime.strptime(f"{date_str} {start_time_str}", "%Y-%m-%d %H:%M")
        end_dt     = start_dt + timedelta(hours=duration_seconds / 3600)
        start_hour = start_dt.hour
        end_hour   = min(int(end_dt.hour), 23)

        r = requests.get("https://archive-api.open-meteo.com/v1/archive", params={
            "latitude": lat, "longitude": lon,
            "start_date": date_str, "end_date": date_str,
            "hourly": ["temperature_2m", "precipitation", "windspeed_10m",
                       "windgusts_10m", "weathercode", "cloudcover"],
            "timezone": "Europe/Zurich",
        })
        data = r.json()
        if "hourly" not in data:
            return None

        hourly       = data["hourly"]
        hours        = hourly["time"]
        ride_indices = [i for i, h in enumerate(hours)
                        if start_hour <= int(h[11:13]) <= end_hour] or list(range(len(hours)))

        def avg(key):
            vals = [hourly[key][i] for i in ride_indices if hourly[key][i] is not None]
            return round(sum(vals) / len(vals), 1) if vals else None

        def total(key):
            vals = [hourly[key][i] for i in ride_indices if hourly[key][i] is not None]
            return round(sum(vals), 1) if vals else None

        codes         = [hourly["weathercode"][i] for i in ride_indices
                         if hourly["weathercode"][i] is not None]
        dominant_code = max(set(codes), key=codes.count) if codes else None

        return {
            "Temp_Avg_C":        avg("temperature_2m"),
            "Precipitation_mm":  total("precipitation"),
            "Wind_Avg_kmh":      avg("windspeed_10m"),
            "Wind_Gusts_kmh":    avg("windgusts_10m"),
            "Cloud_Cover_pct":   avg("cloudcover"),
            "Weather_Condition": weather_description(dominant_code),
            "Rain":              "Yes" if (total("precipitation") or 0) > 0.5 else "No",
        }
    except Exception as e:
        print(f"  ⚠️  Weather error: {e}")
        return None


# ── Classify ride type ─────────────────────────────
def classify_ride_type(title):
    t = title.lower()
    if any(x in t for x in ["indoor", "inside", "zwift"]): return "Indoor"
    if "gravel" in t:                                       return "Gravel"
    if any(x in t for x in ["intense", "race", "camp", "epic"]): return "Intense"
    if any(x in t for x in ["social", "slow", "easy", "debutant"]): return "Social"
    return "Other"


# ── Reliable navigation with retry ────────────────
def goto_with_retry(page, url, retries=3, timeout=45000):
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            page.wait_for_load_state("networkidle", timeout=15000)
            return True
        except Exception as e:
            print(f"  ⚠️  Navigation attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(3 * attempt)
    return False


# ── Scrape individual event ────────────────────────
def scrape_event(page, event, access_token, config):
    url = f"https://www.strava.com/clubs/{config['club_id']}/group_events/{event['id']}"
    try:
        response = page.goto(url, timeout=30000)
        page.wait_for_load_state("networkidle", timeout=10000)
        time.sleep(2)

        if response is None or response.status >= 400:
            print(f"  🚫 Unavailable — marked Cancelled")
            return {"Event_ID": str(event["id"]), "Title": event.get("title", "Unknown"),
                    "Date": event["date"], "Status": "Cancelled",
                    "Scraped_At": datetime.now().strftime("%Y-%m-%d %H:%M")}

        body_text = page.inner_text("body")
        if any(x in body_text.lower() for x in ["page not found", "404", "unavailable"]):
            print(f"  🚫 Error page — marked Cancelled")
            return {"Event_ID": str(event["id"]), "Title": event.get("title", "Unknown"),
                    "Date": event["date"], "Status": "Cancelled",
                    "Scraped_At": datetime.now().strftime("%Y-%m-%d %H:%M")}

        for _ in range(15):
            page.evaluate("window.scrollBy(0, 300)")
            time.sleep(0.3)
        page.evaluate("window.scrollTo(0, 0)")
        time.sleep(1)

        body_text = page.inner_text("body")
        lines     = [l.strip() for l in body_text.split("\n") if l.strip()]

        # Title
        title = event.get("title", "Unknown")
        try:
            h1 = page.query_selector("h1")
            if h1: title = h1.inner_text().strip()
        except Exception: pass

        # Start time
        start_time = "09:00"
        try:
            for line in lines:
                m = re.search(r"at\s+(\d{1,2}):(\d{2})\s*(AM|PM)", line, re.IGNORECASE)
                if m:
                    hour, minute = int(m.group(1)), int(m.group(2))
                    if m.group(3).upper() == "PM" and hour != 12: hour += 12
                    if m.group(3).upper() == "AM" and hour == 12: hour = 0
                    start_time = f"{hour:02d}:{minute:02d}"
                    break
        except Exception: pass

        # Sport / Format / Organizer
        sport = ride_format = organizer = ""
        try:
            for i, line in enumerate(lines):
                if line == "Sport"     and i + 1 < len(lines): sport       = lines[i + 1]
                if line == "Format"    and i + 1 < len(lines): ride_format = lines[i + 1]
                if line == "Organizer" and i - 1 >= 0:         organizer   = lines[i - 1]
        except Exception: pass

        # Attendance + names
        going_count = 0
        athlete_names = []
        try:
            found_el = None
            for sel in [
                "p:has-text('people attended')", "p:has-text('person attended')",
                "p:has-text('people are going')", "p:has-text('person is going')",
                "div:has-text('people attended')", "div:has-text('person attended')",
            ]:
                el = page.query_selector(sel)
                if el:
                    txt = el.inner_text().strip()
                    if re.search(r"\d+\s+(people|person)", txt):
                        numbers     = re.findall(r"\d+", txt)
                        going_count = int(numbers[0]) if numbers else 1
                        found_el    = el
                        break
            if found_el and going_count > 0:
                found_el.click()
                time.sleep(2)
                athlete_els   = page.query_selector_all("div[class*='AthleteListModal_athleteName'] a")
                athlete_names = list(dict.fromkeys([
                    a.inner_text().strip() for a in athlete_els if a.inner_text().strip()
                ]))
                page.keyboard.press("Escape")
                time.sleep(1)
        except Exception as e:
            print(f"  ⚠️  Names error: {e}")

        # Distance
        distance = ""
        try:
            for line in lines:
                m = re.match(r"^Distance\s*:\s*(\d+\.?\d*)\s*km", line, re.IGNORECASE)
                if m: distance = f"{m.group(1)} km"; break
        except Exception: pass
        if not distance:
            try:
                for frame in page.frames:
                    if "strava-embeds.com" in frame.url:
                        fl = [l.strip() for l in frame.inner_text("body").split("\n") if l.strip()]
                        for i, line in enumerate(fl):
                            if line == "Distance" and i + 1 < len(fl):
                                m = re.search(r"(\d+\.?\d*)\s*km", fl[i + 1])
                                if m: distance = f"{m.group(1)} km"; break
                        if distance: break
            except Exception: pass

        # Elevation
        elevation = ""
        try:
            for line in lines:
                m = re.match(r"^(Dénivelé|Elevation gain)\s*:\s*(\d+)\s*m", line, re.IGNORECASE)
                if m: elevation = f"{m.group(2)} m"; break
        except Exception: pass
        if not elevation:
            try:
                for frame in page.frames:
                    if "strava-embeds.com" in frame.url:
                        fl = [l.strip() for l in frame.inner_text("body").split("\n") if l.strip()]
                        for i, line in enumerate(fl):
                            if line == "Elev Gain" and i + 1 < len(fl):
                                m = re.search(r"(\d+[,.]?\d*)\s*m", fl[i + 1])
                                if m: elevation = m.group(1).replace(",", "") + " m"; break
                        if elevation: break
            except Exception: pass

        # Route ID
        route_id = None
        try:
            for frame in page.frames:
                m = re.search(r"strava-embeds\.com/route/(\d+)", frame.url)
                if m: route_id = m.group(1); break
        except Exception: pass

        weather_location      = "fallback_lausanne"
        ride_type             = classify_ride_type(title)
        lat                   = config["fallback_lat"]
        lon                   = config["fallback_lon"]
        estimated_moving_time = 14400

        if route_id:
            try:
                rd                    = get_route_details(route_id, access_token, config)
                lat, lon              = rd["lat"], rd["lon"]
                weather_location      = rd["weather_location"]
                estimated_moving_time = rd["estimated_moving_time"]
                if rd["route_type"] != "Unknown":
                    ride_type = rd["route_type"]
            except Exception as e:
                print(f"  ⚠️  Route error: {e}")

        # Weather (past events only)
        weather    = {}
        event_date = datetime.strptime(event["date"], "%Y-%m-%d")
        if event_date < datetime.now():
            weather = get_weather(lat, lon, event["date"], start_time, estimated_moving_time) or {}

        print(f"  ✅ {event['date']} | {title[:25]:25} | "
              f"Att: {going_count:3} | Names: {len(athlete_names):3} | "
              f"Type: {ride_type:8} | Dist: {distance:8} | Elev: {elevation:6}")

        return {
            "Event_ID":          str(event["id"]),
            "Title":             title,
            "Date":              event["date"],
            "Status":            "Active",
            "Start_Time":        start_time,
            "Route_ID":          route_id or "",
            "Sport":             sport,
            "Format":            ride_format,
            "Ride_Type":         ride_type,
            "Organizer":         organizer,
            "Athletes_Count":    going_count,
            "Names_Fetched":     len(athlete_names),
            "Athletes_Names":    ", ".join(athlete_names),
            "Distance":          distance,
            "Elevation":         elevation,
            "Weather_Location":  weather_location,
            "Temp_Avg_C":        weather.get("Temp_Avg_C"),
            "Precipitation_mm":  weather.get("Precipitation_mm"),
            "Wind_Avg_kmh":      weather.get("Wind_Avg_kmh"),
            "Wind_Gusts_kmh":    weather.get("Wind_Gusts_kmh"),
            "Cloud_Cover_pct":   weather.get("Cloud_Cover_pct"),
            "Weather_Condition": weather.get("Weather_Condition"),
            "Rain":              weather.get("Rain"),
            "Scraped_At":        datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

    except Exception as e:
        print(f"  ❌ {event['date']} | {event.get('title','?')[:30]} | Error: {e}")
        return None


# ── Main ───────────────────────────────────────────
def run():
    config       = load_config()
    access_token = get_valid_token(config)
    events       = fetch_club_events(access_token, config)

    if not events:
        print("❌ No events found.")
        return

    existing_df, existing_ids = load_existing_data()

    to_scrape, skipped = [], 0
    for event in events:
        action = get_event_action(event, existing_ids)
        if action == "skip":
            skipped += 1
        else:
            to_scrape.append((event, action))

    n_refresh = sum(1 for _, a in to_scrape if a == "refresh")
    n_new     = sum(1 for _, a in to_scrape if a == "new")
    print(f"\n📋 Plan: {skipped} skip · {n_refresh} refresh · {n_new} new")

    if not to_scrape:
        print("✅ Everything up to date.")
        return

    cookies = load_cookies()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        context.add_cookies(cookies)
        page = context.new_page()

        if not goto_with_retry(page, "https://www.strava.com/dashboard"):
            print("❌ Could not reach Strava after 3 attempts — check network.")
            browser.close()
            return
        if "login" in page.url:
            print("❌ Session expired — run save_cookies.py in the Strava folder.")
            browser.close()
            return
        print("✅ Session valid")
        print(f"📡 Scraping {len(to_scrape)} events...\n")

        new_results = []
        for i, (event, action) in enumerate(to_scrape):
            icon = "✨" if action == "new" else "🔄"
            print(f"[{i+1:02d}/{len(to_scrape)}] {icon}", end=" ")
            result = scrape_event(page, event, access_token, config)
            if result:
                new_results.append(result)
            time.sleep(1)

        browser.close()

    if not new_results:
        print("❌ No results scraped.")
        return

    new_df        = pd.DataFrame(new_results)
    refreshed_ids = set(new_df["Event_ID"].tolist())
    if not existing_df.empty:
        existing_df = existing_df[~existing_df["Event_ID"].isin(refreshed_ids)]

    final_df = pd.concat([existing_df, new_df], ignore_index=True)
    final_df = final_df.sort_values("Date").reset_index(drop=True)

    CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
    final_df.to_csv(CSV_FILE, index=False)

    print(f"\n✅ Saved → {CSV_FILE}")
    print(f"   Total: {len(final_df)} events "
          f"({skipped} unchanged · {n_refresh} refreshed · {n_new} new)")


if __name__ == "__main__":
    run()
