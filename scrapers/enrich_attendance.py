"""
scrapers/enrich_attendance.py
Enriches historical event attendance with athlete profile data.

Explodes Athletes_Names (comma-separated) → one row per (event, athlete).
Fuzzy-matches each name against athlete_resolved.csv.
Outputs data/real/attendance_enriched.csv.

Match strategy (in order):
  1. Exact norm match      — strips accents/brackets/extra spaces
  2. First name + initial  — "Jean-Luc L." matches "Jean-Luc Lebeau"
  3. Unmatched             — name kept, profile fields empty

Run: python scrapers/enrich_attendance.py
"""

import re
import unicodedata
from pathlib import Path

import pandas as pd

ROOT     = Path(__file__).parent.parent
ATT_CSV  = ROOT / "data/real/historical_attendance.csv"
PROF_CSV = ROOT / "data/real/athlete_resolved.csv"
LB_CSV   = ROOT / "data/real/leaderboard.csv"
OUT_CSV  = ROOT / "data/real/attendance_enriched.csv"


# ── Name normalisation ─────────────────────────────────────────────────────────

def norm(name: str) -> str:
    name = re.sub(r"\(.*?\)", "", str(name))   # strip (echélon), (🇺🇦) etc.
    nfkd = unicodedata.normalize("NFD", name)
    name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", name).strip().lower()


def fuzzy_key(name: str) -> str:
    """First word + first letter of second word. Handles truncated last names."""
    parts = norm(name).split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[1][0]}"
    return parts[0] if parts else ""


# ── Build lookup indices ───────────────────────────────────────────────────────

def build_index(prof: pd.DataFrame) -> tuple[dict, dict]:
    exact  = {}   # norm(name) → row
    fuzzy  = {}   # fuzzy_key(name) → row  (first match wins)
    for _, row in prof.iterrows():
        n = row["Name"]
        k = norm(n)
        f = fuzzy_key(n)
        exact[k] = row
        if f not in fuzzy:
            fuzzy[f] = row
    return exact, fuzzy


def match_name(raw: str, exact: dict, fuzzy: dict) -> tuple[dict | None, str]:
    """Returns (profile_row_or_None, match_type)."""
    k = norm(raw)
    if k in exact:
        return exact[k], "exact"
    f = fuzzy_key(raw)
    if f and f in fuzzy:
        return fuzzy[f], "fuzzy"
    return None, "none"


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    print(f"\n{'='*60}")
    print(f"  ENRICH ATTENDANCE")
    print(f"{'='*60}\n")

    att  = pd.read_csv(ATT_CSV,  dtype=str)
    prof = pd.read_csv(PROF_CSV, dtype=str)
    prof["Weekly_km"] = pd.to_numeric(prof["Weekly_km"], errors="coerce")

    # Leaderboard fallback: athletes known by name but not yet scraped
    lb = pd.read_csv(LB_CSV, dtype=str)
    lb_known = (lb[["Athlete"]].drop_duplicates()
                .rename(columns={"Athlete": "Name"}))
    lb_known["source"] = "leaderboard"
    prof["source"]     = "profile"

    exact_idx, fuzzy_idx = build_index(prof)

    # Secondary index from leaderboard (only used when profile index misses)
    lb_exact, lb_fuzzy = build_index(lb_known)

    print(f"  Events loaded        : {len(att)}")
    print(f"  Profiles loaded      : {len(prof)}")
    print(f"  Leaderboard names    : {len(lb_known)}")
    print(f"  Exact index (profile): {len(exact_idx)}")
    print()

    EVENT_COLS = ["Event_ID", "Title", "Date", "Ride_Type", "Distance", "Elevation"]

    rows = []
    stats = {"exact": 0, "fuzzy": 0, "none": 0}

    for _, evt in att.iterrows():
        raw_names = str(evt.get("Athletes_Names", "") or "")
        names     = [n.strip() for n in raw_names.split(",") if n.strip()]

        for raw in names:
            profile, match_type = match_name(raw, exact_idx, fuzzy_idx)

            # Fallback: leaderboard name match (no profile data, but confirms club member)
            if profile is None:
                lb_profile, lb_match = match_name(raw, lb_exact, lb_fuzzy)
                if lb_profile is not None:
                    match_type = "leaderboard"
                    stats["leaderboard"] = stats.get("leaderboard", 0) + 1
                else:
                    stats["none"] += 1
            else:
                stats[match_type] += 1

            row = {col: evt.get(col, "") for col in EVENT_COLS}
            row["Athlete_Raw"]    = raw
            row["Match_Type"]     = match_type
            row["Athlete_ID"]     = profile.get("Athlete_ID", "") if profile is not None else ""
            row["Matched_Name"]   = profile["Name"]           if profile is not None else ""
            row["Weekly_km"]      = profile.get("Weekly_km", "") if profile is not None else ""
            row["rider_tier"]     = profile.get("rider_tier", "") if profile is not None else ""
            row["primary_bike"]   = profile.get("primary_bike", "") if profile is not None else ""
            row["primary_tier"]   = profile.get("primary_tier", "") if profile is not None else ""
            row["AllTime_km"]     = profile.get("AllTime_km", "") if profile is not None else ""
            row["Avg_Speed_kmh"]  = profile.get("Avg_Speed_kmh", "") if profile is not None else ""
            rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv(OUT_CSV, index=False)

    total = sum(stats.values())
    print(f"  Total athlete-event rows : {total}")
    print(f"  Matched (profile exact)  : {stats.get('exact', 0)}  ({stats.get('exact',0)/total*100:.0f}%)")
    print(f"  Matched (profile fuzzy)  : {stats.get('fuzzy', 0)}  ({stats.get('fuzzy',0)/total*100:.0f}%)")
    print(f"  Matched (leaderboard)    : {stats.get('leaderboard', 0)}  ({stats.get('leaderboard',0)/total*100:.0f}%)")
    print(f"  Unmatched (unknown)      : {stats.get('none', 0)}  ({stats.get('none',0)/total*100:.0f}%)")
    print()

    # Summary: unique athletes seen at events + their tier
    matched = df[df["Match_Type"] != "none"]
    unique  = matched.drop_duplicates("Athlete_ID")
    tier_counts = unique["rider_tier"].value_counts()
    print(f"  Unique athletes matched  : {len(unique)}")
    print(f"  rider_tier breakdown:")
    for tier in ["top", "mid", "entry", "unknown", ""]:
        n = tier_counts.get(tier, 0)
        if n:
            print(f"    {tier or 'n/a':<10} {n}")

    print()
    print(f"  Unmatched names (sample):")
    unmatched = df[df["Match_Type"] == "none"]["Athlete_Raw"].value_counts()
    for name, cnt in unmatched.head(10).items():
        print(f"    {name}  (appeared {cnt}x)")

    print(f"\n  OK -> {OUT_CSV}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    run()
