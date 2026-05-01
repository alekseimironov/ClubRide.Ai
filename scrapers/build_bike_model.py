"""
scrapers/build_bike_model.py
Builds an empirical bike tier model from real scraped cyclist data.

Bike classification: Gemini Flash LLM (see classify_bikes_llm.py)
  category: road | gravel | mtb | indoor | unknown
  tier:     entry | mid | top

Primary bike selection (2+ bikes):
  score = tier_rank + log10(km + 1)
    tier_rank: top=3, mid=2, entry=1
  Only road bikes compete. Gravel/MTB/indoor excluded.
  A top-tier bike needs ~10x fewer km than a mid-tier to still win.

Rider tier = best road-bike tier owned (drives upgrade scoring).

Output:
  data/synthetic/bike_model.json  — conditional tier distributions per km band
  data/real/athlete_resolved.csv  — one record per athlete, all fields resolved

Run order:
  1. python scrapers/classify_bikes_llm.py   (once, cached)
  2. python scrapers/build_bike_model.py
"""

import json
import math
import re
import unicodedata
from datetime import date
from pathlib import Path

import pandas as pd


def _norm(name: str) -> str:
    name = re.sub(r"\(.*?\)", "", str(name))
    nfkd = unicodedata.normalize("NFD", name)
    name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", name).strip().lower()


def merge_duplicate_accounts(profiles: pd.DataFrame,
                              bikes: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Athletes with the same normalised name but different Athlete_IDs are the
    same person using two Strava accounts (e.g. old + active).
    Strategy:
      - Primary account = highest CurrYear_km (most active in 2026)
      - Numeric stats   = summed across both accounts
      - Longest/Biggest = max across both accounts
      - Bikes           = combined pool (secondary account bikes re-tagged to primary ID)
      - Location        = first non-null value
    """
    SUM_COLS = ["AllTime_km", "AllTime_acts", "AllTime_elev_m", "AllTime_time_h",
                "CurrYear_km", "CurrYear_acts", "CurrYear_elev_m", "CurrYear_time_h",
                "Bike_Count"]
    MAX_COLS = ["Longest_Ride_km", "Biggest_Climb_m"]

    for col in SUM_COLS + MAX_COLS:
        if col in profiles.columns:
            profiles[col] = pd.to_numeric(profiles[col], errors="coerce").fillna(0)

    profiles = profiles.copy()
    bikes    = bikes.copy()
    profiles["_norm"] = profiles["Name"].apply(_norm)

    dup_norms = profiles[profiles["_norm"].duplicated(keep=False)]["_norm"].unique()
    if len(dup_norms) == 0:
        return profiles.drop(columns=["_norm"]), bikes

    merged_rows   = []
    secondary_ids = set()

    for norm_name in dup_norms:
        grp = profiles[profiles["_norm"] == norm_name].copy()
        grp = grp.sort_values("CurrYear_km", ascending=False)

        primary    = grp.iloc[0].copy()
        primary_id = str(primary["Athlete_ID"])
        sec_ids    = [str(r["Athlete_ID"]) for _, r in grp.iloc[1:].iterrows()]

        # Sum numeric stats
        for col in SUM_COLS:
            if col in grp.columns:
                primary[col] = grp[col].sum()
        for col in MAX_COLS:
            if col in grp.columns:
                primary[col] = grp[col].max()

        # Location: use first non-empty value across all accounts
        for _, row in grp.iterrows():
            loc = str(row.get("Location", "")).strip()
            if loc and loc.lower() not in ("nan", ""):
                primary["Location"] = loc
                break

        # Re-tag secondary account bikes to primary ID
        for sec_id in sec_ids:
            bikes.loc[bikes["Athlete_ID"].astype(str) == sec_id, "Athlete_ID"] = primary_id
            secondary_ids.add(sec_id)

        merged_rows.append(primary)
        names = grp["Name"].tolist()
        print(f"  Merged accounts: {names} -> primary ID {primary_id}")

    # Remove all duplicate rows, add back merged ones
    clean   = profiles[~profiles["_norm"].isin(dup_norms)].drop(columns=["_norm"])
    merged  = pd.DataFrame(merged_rows).drop(columns=["_norm"], errors="ignore")
    profiles = pd.concat([clean, merged], ignore_index=True)

    return profiles, bikes

ROOT           = Path(__file__).parent.parent
PROFILES       = ROOT / "data/real/athlete_profiles.csv"
BIKES_CSV      = ROOT / "data/real/athlete_bikes.csv"
LLM_CACHE_FILE = ROOT / "data/synthetic/bike_classifications.json"
OUT_JSON       = ROOT / "data/synthetic/bike_model.json"
OUT_CSV        = ROOT / "data/real/athlete_resolved.csv"

WEEKS_ELAPSED = max(1, (date.today() - date(date.today().year, 1, 1)).days // 7)
TIER_RANK     = {"top": 3, "mid": 2, "entry": 1}


# ── LLM cache ─────────────────────────────────────────────────────────────────

def load_llm_cache() -> dict:
    try:
        with open(LLM_CACHE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return {
            f"{e['brand'].strip().lower()}|{e['name'].strip().lower()}": e
            for e in data
        }
    except FileNotFoundError:
        print("  WARNING: bike_classifications.json not found.")
        print("  Run classify_bikes_llm.py first for best results.\n")
        return {}


def lookup_bike(brand: str, name: str, cache: dict) -> tuple[str, str | None]:
    """Returns (category, tier). Defaults: unknown/entry if not in cache."""
    key   = f"{brand.strip().lower()}|{name.strip().lower()}"
    entry = cache.get(key)
    if entry:
        cat  = entry.get("category", "unknown")
        tier = entry.get("tier")
        if tier is None and cat == "road":
            tier = "entry"   # road bike with no tier → conservative default
        return cat, tier
    return "unknown", "entry"


# ── Primary bike resolver ──────────────────────────────────────────────────────

def resolve_primary(ath_bikes: pd.DataFrame, llm_cache: dict) -> dict:
    if ath_bikes.empty:
        return {"primary_bike": "", "primary_brand": "", "primary_cat": "unknown",
                "primary_tier": "unknown", "primary_bike_km": 0,
                "fleet_km": 0, "bike_count": 0}

    ath_bikes = ath_bikes.copy()
    ath_bikes["Bike_Km"] = pd.to_numeric(ath_bikes["Bike_Km"], errors="coerce").fillna(0)

    cats, tiers = zip(*[
        lookup_bike(str(r.get("Brand", "")), str(r.get("Bike_Name", "")), llm_cache)
        for _, r in ath_bikes.iterrows()
    ])
    ath_bikes["Category"] = cats
    ath_bikes["Tier"]     = tiers

    # All real bikes compete for primary — only indoor trainers excluded
    # Gravel/MTB get tier_rank=1 (entry) via null tier, so they win only through km volume
    road = ath_bikes[ath_bikes["Category"] != "indoor"].copy()

    if road.empty:
        # Athlete has only indoor trainers — pick highest km overall
        primary = ath_bikes.sort_values("Bike_Km", ascending=False).iloc[0]
    else:
        road["_rank"]  = road["Tier"].map(TIER_RANK).fillna(1)
        road["_score"] = road["_rank"] + road["Bike_Km"].apply(
            lambda km: math.log10(km + 1)
        )
        primary = road.sort_values("_score", ascending=False).iloc[0]

    return {
        "primary_bike":    str(primary.get("Bike_Name", "")),
        "primary_brand":   str(primary.get("Brand", "")),
        "primary_cat":     str(primary.get("Category", "unknown")),
        "primary_tier":    str(primary.get("Tier") or "entry"),
        "primary_bike_km": round(float(primary.get("Bike_Km", 0) or 0), 0),
        "fleet_km":        round(float(ath_bikes["Bike_Km"].sum()), 0),
        "bike_count":      len(ath_bikes),
    }


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    print(f"\n{'='*60}")
    print(f"  BUILD BIKE MODEL  (predictor: weekly_km)")
    print(f"  Weeks elapsed in {date.today().year}: {WEEKS_ELAPSED}")
    print(f"{'='*60}\n")

    profiles  = pd.read_csv(PROFILES, dtype=str)
    bikes     = pd.read_csv(BIKES_CSV, dtype=str)
    llm_cache = load_llm_cache()
    print(f"  LLM cache entries    : {len(llm_cache)}")

    # Merge same-person duplicate accounts before processing
    profiles, bikes = merge_duplicate_accounts(profiles, bikes)
    print()

    records = []
    for _, row in profiles.iterrows():
        aid       = str(row["Athlete_ID"])
        ath_bikes = bikes[bikes["Athlete_ID"].astype(str) == aid]
        resolved  = resolve_primary(ath_bikes, llm_cache)

        alltime_km   = float(row.get("AllTime_km")   or 0)
        curr_km      = float(row.get("CurrYear_km")  or 0)
        alltime_time = float(row.get("AllTime_time_h") or 0)
        alltime_acts = float(row.get("AllTime_acts")  or 0)
        longest      = float(row.get("Longest_Ride_km") or 0)

        weekly_km  = round(curr_km / WEEKS_ELAPSED, 1) if curr_km > 0 else 0
        avg_speed  = round(alltime_km / alltime_time, 1) if alltime_time > 0 else 0
        avg_ride_h = round(alltime_time / alltime_acts, 2) if alltime_acts > 0 else 0

        # rider_tier: behavioral intensity from 2026 weekly km
        if weekly_km == 0:
            rider_tier = "unknown"   # no 2026 data
        elif weekly_km < 50:
            rider_tier = "entry"     # occasional rider
        elif weekly_km < 150:
            rider_tier = "mid"       # regular cyclist
        else:
            rider_tier = "top"       # serious training

        records.append({
            "Athlete_ID":      aid,
            "Name":            str(row.get("Name", "")),
            "Location":        str(row.get("Location", "")),
            "AllTime_km":      alltime_km,
            "CurrYear_km":     curr_km,
            "Weekly_km":       weekly_km,
            "AllTime_time_h":  alltime_time,
            "Avg_Speed_kmh":   avg_speed,
            "Avg_Ride_h":      avg_ride_h,
            "Longest_Ride_km": longest,
            "rider_tier":      rider_tier,
            **resolved,
        })

    df = pd.DataFrame(records)

    # Road-tier athletes only for population stats
    df_known = df[df["rider_tier"].isin(["entry", "mid", "top"])].copy()
    df_known["Weekly_km"] = pd.to_numeric(df_known["Weekly_km"], errors="coerce").fillna(0)

    print(f"  Athletes total        : {len(df)}")
    print(f"  With known road tier  : {len(df_known)}")

    # ── Tier distribution ──────────────────────────────────────────────────────
    tier_counts = df_known["rider_tier"].value_counts()
    total       = len(df_known)
    tier_dist   = {t: round(tier_counts.get(t, 0) / total * 100, 1)
                   for t in ["entry", "mid", "top"]}

    print(f"\n  Overall tier distribution (Swiss cyclists):")
    for tier, pct in tier_dist.items():
        bar = "#" * min(int(pct / 2), 30)
        print(f"    {tier:<8}  {bar:<30}  {pct:.0f}%  (n={tier_counts.get(tier, 0)})")

    # ── Conditional distribution by weekly km band ────────────────────────────
    KM_BANDS = [
        ("high",   150, 9999),
        ("medium", 50,  150),
        ("low",      0,  50),
    ]
    conditional = {}
    print(f"\n  Conditional tier distribution by weekly km:")
    print(f"  {'Band':<10} {'Range':>12}   {'entry':>6} {'mid':>6} {'top':>6}  n")
    for band_name, lo, hi in KM_BANDS:
        mask = (df_known["Weekly_km"] >= lo) & (df_known["Weekly_km"] < hi)
        grp  = df_known[mask]
        n    = len(grp)
        if n == 0:
            conditional[band_name] = tier_dist
            continue
        dist = {t: round(grp[grp["rider_tier"] == t].shape[0] / n * 100, 1)
                for t in ["entry", "mid", "top"]}
        conditional[band_name] = dist
        print(f"  {band_name:<10} {lo:>5}-{hi:>4} km/wk  "
              f"{dist['entry']:>5.0f}% {dist['mid']:>5.0f}% {dist['top']:>5.0f}%  n={n}")

    # ── Brand distribution per tier ────────────────────────────────────────────
    merged = df_known.merge(
        bikes[bikes["Brand"].notna()][["Athlete_ID", "Brand"]].drop_duplicates(),
        on="Athlete_ID", how="left"
    )
    tier_brands = {}
    print(f"\n  Top brands by tier:")
    for tier in ["entry", "mid", "top"]:
        bc = merged[merged["rider_tier"] == tier]["Brand"].value_counts()
        bc = bc[~bc.index.str.lower().isin(
            ["unknown", "other", "race", "road", "gravel", "indoor", ""]
        )]
        tier_brands[tier] = {b: int(c) for b, c in bc.items()}
        top5 = ", ".join(f"{b}({c})" for b, c in bc.head(5).items())
        print(f"    {tier:<8}: {top5}")

    # ── Save model ─────────────────────────────────────────────────────────────
    model = {
        "description":   "Empirical bike model — Swiss cyclists, real Strava data",
        "classifier":    "Gemini Flash LLM (bike_classifications.json)",
        "sample_size":   len(df_known),
        "weeks_elapsed": WEEKS_ELAPSED,
        "tier_dist":     tier_dist,
        "conditional":   conditional,
        "tier_brands":   tier_brands,
        "km_bands": {
            "high":   {"min": 150, "label": "serious (>150 km/wk)"},
            "medium": {"min": 50, "label": "regular (50-150 km/wk)"},
            "low":    {"min":   0, "label": "casual (<50 km/wk)"},
        },
        "note": "Tier assigned probabilistically — never state as fact in bot responses",
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_JSON, "w") as f:
        json.dump(model, f, indent=2)

    df.to_csv(OUT_CSV, index=False)

    print(f"\n  OK bike_model.json  -> {OUT_JSON}")
    print(f"  OK athlete_resolved -> {OUT_CSV}")
    print(f"\n  Run next: python scrapers/generate_crm.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
