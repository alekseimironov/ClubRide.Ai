"""
scrapers/generate_crm.py
Generates realistic synthetic CRM records for all 162 TNCE event attendees.

Sources:
  data/real/attendance_enriched.csv  — 162 unique attendees, match status, event count
  data/real/athlete_resolved.csv     — real bike + km for 75 profile-matched athletes
  data/synthetic/bike_model.json     — conditional tier distribution for unmatched athletes
  config.json                        — service intervals

Logic per athlete:
  Profile matched (75):
    - Bike:     real primary_bike + primary_tier from Strava scrape
    - Km:       real AllTime_km from Strava profile
    - Weekly:   real Weekly_km
  Leaderboard-only (18) or unknown (69):
    - Bike:     synthetic, tier drawn from attendance-based probability
    - Km:       estimated from attendance count x average weekly km

Purchase date:
  - First event appearance - random 1-24 months (wider range = more variety)
  - 25% purchased from Club BSL (flagged, slightly better tier)

Service history:
  - 40% riders overdue (km_since_service > service_interval)
  - Chain: 50% overdue

Output: data/synthetic/crm.csv
"""

import json
import random
import re
import unicodedata
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT       = Path(__file__).parent.parent
ENR_CSV    = ROOT / "data/real/attendance_enriched.csv"
PROF_CSV   = ROOT / "data/real/athlete_resolved.csv"
MODEL_FILE = ROOT / "data/synthetic/bike_model.json"
CFG_FILE   = ROOT / "config.json"
OUT_CSV    = ROOT / "data/synthetic/crm.csv"

random.seed(42)

CLUB_NAME = "Club TNCE"

# ── Curated bike catalogue per tier ───────────────────────────────────────────
BIKES_BY_TIER = {
    "entry": [
        ("Trek",        "Domane AL 4"),
        ("Specialized", "Allez E5"),
        ("Giant",       "Contend AR 3"),
        ("Scott",       "Speedster 50"),
        ("Cannondale",  "Synapse Al"),
        ("Decathlon",   "Van Rysel RC"),
        ("Decathlon",   "Triban RC 520"),
        ("Bianchi",     "Via Nirone 7"),
        ("Merida",      "Ride 60"),
        ("Cube",        "Attain GTC SL"),
    ],
    "mid": [
        ("Trek",        "Emonda SL 6"),
        ("Trek",        "Domane SL 6"),
        ("Specialized", "Tarmac SL7"),
        ("Specialized", "Roubaix Sport"),
        ("Canyon",      "Endurace CF SL 7"),
        ("Canyon",      "Ultimate CF SL 7"),
        ("Giant",       "TCR Advanced 2"),
        ("Scott",       "Addict RC 30"),
        ("Cannondale",  "SuperSix EVO 3"),
        ("BMC",         "Roadmachine 02"),
        ("Cervelo",     "Caledonia 5"),
        ("Orbea",       "Orca M35i"),
        ("Merida",      "Scultura 6000"),
        ("Lapierre",    "Xelius SL 600"),
        ("Cube",        "Agree C:62 SL"),
    ],
    "top": [
        ("Trek",        "Madone SLR 7"),
        ("Trek",        "Emonda SLR 7"),
        ("Specialized", "S-Works Tarmac SL8"),
        ("Specialized", "S-Works Aethos"),
        ("Canyon",      "Aeroad CF SLX 8"),
        ("Canyon",      "Ultimate CFR"),
        ("Look",        "795 Blade RS"),
        ("Pinarello",   "Dogma F"),
        ("Colnago",     "V3Rs"),
        ("Wilier",      "Cento10 SL"),
        ("Cervelo",     "R5 Disc"),
        ("BMC",         "Teammachine SLR01"),
        ("Scott",       "Addict RC Pro"),
        ("Cannondale",  "SuperSix EVO Hi-Mod"),
        ("Factor",      "One Disc"),
        ("OPEN",        "WI.DE"),
    ],
}

TIER_RANK = {"top": 3, "mid": 2, "entry": 1}


# ── Helpers ───────────────────────────────────────────────────────────────────

def norm(name: str) -> str:
    name = re.sub(r"\(.*?\)", "", str(name))
    nfkd = unicodedata.normalize("NFD", name)
    name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", name).strip().lower()


def pick_bike(tier: str) -> tuple[str, str]:
    return random.choice(BIKES_BY_TIER.get(tier, BIKES_BY_TIER["mid"]))


def draw_tier_from_attendance(events_count: int, model: dict) -> str:
    """
    For athletes without profile data, estimate tier from attendance frequency.
    More events = more serious = higher tier probability.
    """
    if events_count >= 10:
        band = "high"
    elif events_count >= 4:
        band = "medium"
    else:
        band = "low"

    dist = model["conditional"].get(band, model["tier_dist"])
    r = random.uniform(0, 100)
    if r < dist["entry"]:
        return "entry"
    if r < dist["entry"] + dist["mid"]:
        return "mid"
    return "top"


def parse_date(date_str: str) -> date | None:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return pd.to_datetime(date_str, format=fmt).date()
        except Exception:
            pass
    try:
        return pd.to_datetime(date_str, dayfirst=False).date()
    except Exception:
        return None


def generate_service_record(total_km: float, weekly_km: float,
                             purchase_date: date, service_km: int,
                             chain_km: int) -> dict:
    today = date.today()
    total_km = max(total_km, 1)

    # 40% riders overdue on service
    overdue = random.random() < 0.40
    if overdue:
        km_since_service = round(random.uniform(service_km, service_km * 1.8), 0)
    else:
        km_since_service = round(random.uniform(0, service_km * 0.80), 0)

    # Back-calculate last service date from km rate
    weekly = max(weekly_km, 10)
    weeks_ago = km_since_service / weekly
    last_service_date = today - timedelta(days=int(weeks_ago * 7))
    last_service_date = max(last_service_date, purchase_date)

    # Chain: 50% overdue
    chain_overdue = random.random() < 0.50
    if chain_overdue:
        km_since_chain = round(random.uniform(chain_km, chain_km * 1.8), 0)
    else:
        km_since_chain = round(random.uniform(0, chain_km * 0.80), 0)

    return {
        "Last_Service_Date": last_service_date.isoformat(),
        "Km_Since_Service":  km_since_service,
        "Service_Due":       km_since_service >= service_km,
        "Km_Since_Chain":    km_since_chain,
        "Chain_Due":         km_since_chain >= chain_km,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    print(f"\n{'='*60}")
    print(f"  GENERATE CRM  (162 event attendees)")
    print(f"{'='*60}\n")

    with open(CFG_FILE) as f:
        cfg = json.load(f)
    service_km = cfg["bike_service"]["service_interval_km"]
    chain_km   = cfg["bike_service"]["chain_replace_km"]
    print(f"  Service interval : {service_km:,} km")
    print(f"  Chain interval   : {chain_km:,} km")

    with open(MODEL_FILE) as f:
        model = json.load(f)
    print(f"  Bike model       : loaded (sample={model['sample_size']})\n")

    enr  = pd.read_csv(ENR_CSV,  dtype=str)
    prof = pd.read_csv(PROF_CSV, dtype=str)

    prof["Weekly_km"]       = pd.to_numeric(prof["Weekly_km"],       errors="coerce").fillna(0)
    prof["AllTime_km"]      = pd.to_numeric(prof["AllTime_km"],      errors="coerce").fillna(0)
    prof["primary_bike_km"] = pd.to_numeric(prof["primary_bike_km"], errors="coerce").fillna(0)

    # Build profile lookup by Athlete_ID
    prof_by_id   = prof.set_index("Athlete_ID").to_dict("index")
    prof_by_name = {norm(row["Name"]): row for row in prof.to_dict("records")}

    # Attendance frequency + first/last event per athlete
    enr["_date"] = pd.to_datetime(enr["Date"], dayfirst=False, errors="coerce")
    freq = (enr.groupby("Athlete_Raw")
            .agg(Events_Count=("Event_ID", "count"),
                 First_Event=("_date", "min"),
                 Last_Event=("_date", "max"))
            .reset_index())

    # Unique attendees — deduplicate by normalised name (same person, 2 accounts)
    unique = (enr.drop_duplicates("Athlete_Raw")
              [["Athlete_Raw", "Match_Type", "Matched_Name", "Athlete_ID",
                "rider_tier", "primary_bike", "primary_tier"]]
              .copy())
    unique = unique.merge(freq, on="Athlete_Raw", how="left")
    unique["_norm"] = unique["Athlete_Raw"].apply(norm)
    # Keep highest event count per normalised name (dedup same person)
    unique = (unique.sort_values("Events_Count", ascending=False)
              .drop_duplicates("_norm", keep="first")
              .reset_index(drop=True))

    print(f"  Unique attendees (deduped) : {len(unique)}")
    print(f"  Profile match              : {(unique['Match_Type']=='exact').sum() + (unique['Match_Type']=='fuzzy').sum()}")
    print(f"  Leaderboard only           : {(unique['Match_Type']=='leaderboard').sum()}")
    print(f"  Unknown                    : {(unique['Match_Type']=='none').sum()}\n")

    records = []
    stats   = {"real_bike": 0, "synthetic_bike": 0, "club_bsl": 0}

    for _, row in unique.iterrows():
        match_type   = str(row.get("Match_Type", "none"))
        ec = row.get("Events_Count")
        events_count = 1 if (ec is None or str(ec).lower() == 'nan') else int(float(ec))
        first_event  = row.get("First_Event")
        if pd.isna(first_event):
            first_seen = date(2025, 10, 1)
        else:
            first_seen = pd.Timestamp(first_event).date()

        # ── Bike assignment ────────────────────────────────────────────────────
        if match_type in ("exact", "fuzzy"):
            # Real data from Strava profile
            aid          = str(row.get("Athlete_ID", ""))
            profile_data = prof_by_id.get(aid) or prof_by_name.get(norm(str(row.get("Matched_Name", ""))))

            if profile_data:
                real_bike  = str(profile_data.get("primary_bike") or "")
                real_brand = str(profile_data.get("primary_brand") or "")
                real_tier  = str(profile_data.get("primary_tier") or "mid")
                weekly_km  = float(profile_data.get("Weekly_km") or 50)
                total_km   = float(profile_data.get("AllTime_km") or 5000)
                rider_tier = str(profile_data.get("rider_tier") or "mid")

                if real_bike and real_bike not in ("", "nan"):
                    bike_brand = real_brand if real_brand not in ("", "nan") else real_bike.split()[0]
                    bike_model = real_bike
                    bike_tier  = real_tier if real_tier in TIER_RANK else "mid"
                    stats["real_bike"] += 1
                else:
                    # Profile exists but no bike scraped yet
                    bike_tier           = rider_tier if rider_tier in TIER_RANK else "mid"
                    bike_brand, bike_model = pick_bike(bike_tier)
                    bike_tier           = bike_tier
                    stats["synthetic_bike"] += 1
            else:
                bike_tier           = draw_tier_from_attendance(events_count, model)
                bike_brand, bike_model = pick_bike(bike_tier)
                weekly_km  = 60.0
                total_km   = events_count * 60 * 10
                rider_tier = bike_tier
                stats["synthetic_bike"] += 1

        else:
            # Leaderboard-only or unknown — fully synthetic
            bike_tier           = draw_tier_from_attendance(events_count, model)
            bike_brand, bike_model = pick_bike(bike_tier)
            # Estimate km from events attended
            weekly_km  = {"top": 180, "mid": 90, "entry": 30}.get(bike_tier, 60)
            total_km   = events_count * weekly_km * 4   # rough estimate
            rider_tier = bike_tier
            stats["synthetic_bike"] += 1

        # ── Purchase date — calculated from primary_bike_km / weekly_km ──────────
        # For profile-matched athletes: derive from actual bike km + riding rate
        # For others: random 1-36 months before first event
        today = date.today()
        if match_type in ("exact", "fuzzy") and profile_data:
            primary_bike_km = float(profile_data.get("primary_bike_km") or 0)
            wk = max(float(profile_data.get("Weekly_km") or 0), 20)  # floor 20km/wk
            if primary_bike_km > 0:
                weeks_riding = primary_bike_km / wk
                # Cap: min 3 months, max 7 years
                weeks_riding = min(max(weeks_riding, 13), 365)
                purchase_date = today - timedelta(weeks=weeks_riding)
                # Small random jitter ±2 weeks for realism
                purchase_date += timedelta(days=random.randint(-14, 14))
            else:
                # No bike km — use first_event - random 3-18 months
                purchase_date = first_seen - timedelta(days=random.randint(90, 540))
        else:
            max_months = 36 if bike_tier == "top" else 24
            months_before = random.randint(1, max_months)
            purchase_date = first_seen - timedelta(days=months_before * 30)

        purchase_date = max(purchase_date, date(2015, 1, 1))

        # ── Purchase source ────────────────────────────────────────────────────
        # 25% bought through Club BSL (more likely for top-tier and frequent members)
        club_prob = 0.40 if (bike_tier == "top" or events_count >= 8) else 0.20
        from_club = random.random() < club_prob
        purchase_source = CLUB_NAME if from_club else "External"
        if from_club:
            stats["club_bsl"] += 1

        # ── Service record ─────────────────────────────────────────────────────
        svc = generate_service_record(
            total_km, weekly_km, purchase_date, service_km, chain_km
        )

        records.append({
            "Athlete":          str(row.get("Athlete_Raw", "")),
            "Match_Type":       match_type,
            "rider_tier":       rider_tier,
            "Events_Count":     events_count,
            "First_Event":      first_seen.isoformat(),
            "Last_Event":       pd.Timestamp(row["Last_Event"]).date().isoformat()
                                if pd.notna(row.get("Last_Event")) else "",
            "Bike_Brand":       bike_brand,
            "Bike_Model":       bike_model,
            "Bike_Tier":        bike_tier,
            "Purchase_Date":    purchase_date.isoformat(),
            "Purchase_Source":  purchase_source,
            "Total_Est_Km":     round(total_km, 0),
            "Weekly_Est_Km":    round(weekly_km, 1),
            **svc,
            "Data_Source":      "real_profile" if match_type in ("exact","fuzzy") else "synthetic",
        })

    df = pd.DataFrame(records).sort_values("Events_Count", ascending=False).reset_index(drop=True)
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False)

    service_due = df["Service_Due"].sum()
    chain_due   = df["Chain_Due"].sum()
    tier_counts = df["Bike_Tier"].value_counts()

    print(f"  Records saved      : {len(df)} -> {OUT_CSV.name}")
    print(f"  Real bike data     : {stats['real_bike']}")
    print(f"  Synthetic bike     : {stats['synthetic_bike']}")
    print(f"  Purchased Club BSL : {stats['club_bsl']}")
    print(f"  Service due        : {service_due}")
    print(f"  Chain due          : {chain_due}")
    print(f"  Tier dist          : entry={tier_counts.get('entry',0)}  mid={tier_counts.get('mid',0)}  top={tier_counts.get('top',0)}")
    print(f"\n  Top 15 by event attendance:")
    cols = ["Athlete","rider_tier","Bike_Brand","Bike_Model","Bike_Tier",
            "Events_Count","Purchase_Source","Service_Due"]
    print(df[cols].head(15).to_string(index=False))
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    run()
