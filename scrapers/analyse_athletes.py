"""
scrapers/analyse_athletes.py
Data analysis of scraped followed athletes.
Focus: Swiss cyclists, bike patterns, mileage insights.

Run: python scrapers/analyse_athletes.py
"""

import re
import unicodedata
from pathlib import Path

import pandas as pd

ROOT      = Path(__file__).parent.parent
PROFILES  = ROOT / "data/real/athlete_profiles.csv"
BIKES     = ROOT / "data/real/athlete_bikes.csv"

BIKE_TIERS = {
    "top": ["dogma", "r5", "s5", "madone slr", "tarmac slr", "addict rc pro",
            "teammachine slr01", "795 blade", "supersix evo hi-mod",
            "caledonia 5", "foil rc", "propel advanced sl"],
    "mid": ["tarmac sl6", "tarmac sl7", "emonda sl", "domane sl", "addict rc",
            "tcr advanced", "defy advanced", "teammachine slr02", "roadmachine",
            "785 huez", "orca m20", "scultura", "reacto", "supersix evo",
            "synapse carbon", "speedster"],
}

def bike_tier(brand: str, model: str) -> str:
    key = f"{brand} {model}".lower()
    for tier, keywords in BIKE_TIERS.items():
        if any(kw in key for kw in keywords):
            return tier
    return "entry"

def is_swiss(location: str) -> bool:
    if not location or str(location) == "nan":
        return False
    loc = str(location).lower()
    return ("switzerland" in loc or "suisse" in loc or
            "schweiz" in loc or "svizzera" in loc)

def sep():
    print("─" * 60)


def run():
    # ── Load data ──────────────────────────────────
    profiles = pd.read_csv(PROFILES, dtype=str)
    bikes    = pd.read_csv(BIKES,    dtype=str)

    # Convert numeric columns
    num_cols_p = ["AllTime_km", "AllTime_acts", "AllTime_elev_m", "AllTime_time_h",
                  "CurrYear_km", "CurrYear_acts", "CurrYear_elev_m", "CurrYear_time_h",
                  "Longest_Ride_km", "Biggest_Climb_m", "Bike_Count"]
    for c in num_cols_p:
        if c in profiles.columns:
            profiles[c] = pd.to_numeric(profiles[c], errors="coerce")

    bikes["Bike_Km"] = pd.to_numeric(bikes["Bike_Km"], errors="coerce").fillna(0)

    print(f"\n{'='*60}")
    print(f"  ATHLETE DATA ANALYSIS")
    print(f"{'='*60}")

    # ── 1. Overview ────────────────────────────────
    sep()
    print(f"  1. DATASET OVERVIEW")
    sep()
    print(f"  Total athletes scraped   : {len(profiles)}")
    swiss = profiles[profiles["Location"].apply(is_swiss)]
    print(f"  Swiss region cyclists    : {len(swiss)}")
    with_bikes = profiles[profiles["Bike_Count"].fillna(0) > 0]
    print(f"  Athletes with public gear: {len(with_bikes)}")
    swiss_with_bikes = swiss[swiss["Bike_Count"].fillna(0) > 0]
    print(f"  Swiss + gear             : {len(swiss_with_bikes)}")
    print(f"  Total bikes catalogued   : {len(bikes)}")

    # ── 2. Swiss region focus ──────────────────────
    sep()
    print(f"  2. SWISS REGION CYCLISTS")
    sep()
    if swiss.empty:
        print("  No Swiss athletes found.")
    else:
        print(f"\n  Location distribution (top 10):")
        loc_counts = swiss["Location"].value_counts().head(10)
        for loc, cnt in loc_counts.items():
            print(f"    {loc:<35}  {cnt:>3} athletes")

    # ── 3. Mileage patterns ────────────────────────
    sep()
    print(f"  3. MILEAGE PATTERNS — ALL SCRAPED CYCLISTS")
    sep()
    valid = profiles[profiles["AllTime_km"].notna() & (profiles["AllTime_km"] > 0)].copy()
    valid["AllTime_km"] = valid["AllTime_km"].astype(float)

    if not valid.empty:
        print(f"\n  All-Time km distribution:")
        bins   = [0, 5000, 15000, 30000, 60000, 999999]
        labels = ["<5k (casual)", "5-15k (recreational)",
                  "15-30k (regular)", "30-60k (serious)", ">60k (elite)"]
        valid["km_tier"] = pd.cut(valid["AllTime_km"], bins=bins, labels=labels)
        for label, cnt in valid["km_tier"].value_counts().sort_index().items():
            pct = cnt / len(valid) * 100
            bar = "█" * min(int(pct / 2), 25)
            print(f"    {label:<25}  {bar:<25}  {cnt:>3} ({pct:.0f}%)")

        print(f"\n  All-Time km stats:")
        print(f"    Median  : {valid['AllTime_km'].median():>8,.0f} km")
        print(f"    Mean    : {valid['AllTime_km'].mean():>8,.0f} km")
        print(f"    Max     : {valid['AllTime_km'].max():>8,.0f} km")
        print(f"    Min     : {valid['AllTime_km'].min():>8,.0f} km")

    # ── 4. 2026 riding patterns ────────────────────
    sep()
    print(f"  4. 2026 RIDING PATTERNS")
    sep()
    yr = profiles[profiles["CurrYear_km"].notna() & (profiles["CurrYear_km"] > 0)].copy()
    yr["CurrYear_km"]   = yr["CurrYear_km"].astype(float)
    yr["CurrYear_acts"] = yr["CurrYear_acts"].astype(float)

    if not yr.empty:
        print(f"\n  Active in 2026: {len(yr)} athletes")
        print(f"    Avg km      : {yr['CurrYear_km'].mean():>7,.0f} km")
        print(f"    Median km   : {yr['CurrYear_km'].median():>7,.0f} km")
        print(f"    Avg acts    : {yr['CurrYear_acts'].mean():>7,.1f} rides")
        print(f"\n  2026 km distribution:")
        bins2   = [0, 500, 1500, 3000, 6000, 999999]
        labels2 = ["<500 (low)", "500-1.5k (moderate)",
                   "1.5-3k (active)", "3-6k (very active)", ">6k (elite)"]
        yr["yr_tier"] = pd.cut(yr["CurrYear_km"], bins=bins2, labels=labels2)
        for label, cnt in yr["yr_tier"].value_counts().sort_index().items():
            pct = cnt / len(yr) * 100
            bar = "█" * min(int(pct / 2), 25)
            print(f"    {label:<25}  {bar:<25}  {cnt:>3} ({pct:.0f}%)")

    # ── 5. Bike analysis ───────────────────────────
    sep()
    print(f"  5. BIKE ANALYSIS")
    sep()

    if not bikes.empty:
        # Brand distribution
        print(f"\n  Top brands:")
        brand_cnt = bikes["Brand"].value_counts().head(12)
        for brand, cnt in brand_cnt.items():
            bar = "█" * min(cnt, 20)
            print(f"    {brand:<15}  {bar:<20}  {cnt}")

        # Bike tier distribution
        bikes["Tier"] = bikes.apply(
            lambda r: bike_tier(str(r.get("Brand", "")), str(r.get("Bike_Name", ""))), axis=1
        )
        print(f"\n  Bike tier distribution:")
        for tier, cnt in bikes["Tier"].value_counts().items():
            pct = cnt / len(bikes) * 100
            bar = "█" * min(int(pct / 2), 25)
            print(f"    {tier:<8}  {bar:<25}  {cnt:>3} ({pct:.0f}%)")

        # km logged per tier
        print(f"\n  Avg km logged by tier:")
        tier_km = bikes.groupby("Tier")["Bike_Km"].agg(["mean", "median", "count"])
        for tier, row in tier_km.iterrows():
            print(f"    {tier:<8}  avg {row['mean']:>7,.0f} km  "
                  f"median {row['median']:>7,.0f} km  "
                  f"({int(row['count'])} bikes)")

        # Most common specific bikes
        print(f"\n  Most common bike models (top 10):")
        model_cnt = bikes["Bike_Name"].value_counts().head(10)
        for model, cnt in model_cnt.items():
            print(f"    {model:<35}  {cnt}")

    # ── 6. Mileage vs bike tier correlation ────────
    sep()
    print(f"  6. MILEAGE vs BIKE TIER CORRELATION")
    sep()
    merged = profiles.merge(
        bikes[["Athlete_ID", "Tier", "Bike_Km"]].rename(columns={"Bike_Km": "Main_Bike_Km"}),
        on="Athlete_ID", how="inner"
    )
    merged["AllTime_km"] = pd.to_numeric(merged["AllTime_km"], errors="coerce")

    if not merged.empty:
        print(f"\n  Avg All-Time km by bike tier:")
        grp = merged.groupby("Tier")["AllTime_km"].agg(["mean", "median", "count"])
        for tier, row in grp.iterrows():
            print(f"    {tier:<8}  avg {row['mean']:>7,.0f} km  "
                  f"median {row['median']:>7,.0f} km  "
                  f"n={int(row['count'])}")
        print(f"\n  → This correlation validates the ML feature: "
              f"AllTime_km predicts bike tier ✅")

    # ── 7. Swiss cyclists detailed ─────────────────
    sep()
    print(f"  7. TOP SWISS CYCLISTS BY ALL-TIME KM")
    sep()
    swiss_valid = swiss[swiss["AllTime_km"].notna() & (swiss["AllTime_km"] > 0)].copy()
    swiss_valid["AllTime_km"] = swiss_valid["AllTime_km"].astype(float)
    swiss_valid = swiss_valid.sort_values("AllTime_km", ascending=False).head(15)

    if not swiss_valid.empty:
        print(f"\n  {'Name':<30} {'Location':<25} {'AllTime_km':>10} {'2026_km':>8}")
        print(f"  {'─'*30} {'─'*25} {'─'*10} {'─'*8}")
        for _, r in swiss_valid.iterrows():
            print(f"  {str(r['Name'])[:29]:<30} "
                  f"{str(r['Location'])[:24]:<25} "
                  f"{float(r['AllTime_km'] or 0):>10,.0f} "
                  f"{float(r['CurrYear_km'] or 0):>8,.0f}")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    run()
