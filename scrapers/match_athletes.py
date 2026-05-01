"""
scrapers/match_athletes.py
Step 1: Cross-reference followed athletes with TNCE members.

Finds athletes who appear in BOTH:
  - athlete_profiles.csv  (scraped from Strava following list)
  - historical_attendance.csv OR leaderboard.csv (TNCE club data)

Uses fuzzy name matching (first name + last initial) to handle
format differences between datasets.

Output:
  data/real/matched_athletes.csv  — confirmed overlaps with real data
  Console report                  — match quality + key stats

Run: python scrapers/match_athletes.py
"""

import re
import unicodedata
from pathlib import Path

import pandas as pd

ROOT       = Path(__file__).parent.parent
PROFILES   = ROOT / "data/real/athlete_profiles.csv"
BIKES      = ROOT / "data/real/athlete_bikes.csv"
ATTENDANCE = ROOT / "data/real/historical_attendance.csv"
LEADERBOARD= ROOT / "data/real/leaderboard.csv"
OUT_CSV    = ROOT / "data/real/matched_athletes.csv"


# ── Name normalisation ─────────────────────────────
def norm(name: str) -> str:
    """Lowercase + strip diacritics + remove parentheticals."""
    name = re.sub(r"\(.*?\)", "", str(name))
    nfkd = unicodedata.normalize("NFD", name)
    name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", name).strip().lower()

def fuzzy_key(name: str) -> str:
    """firstname + last_initial — handles 'João B.' vs 'João Baptista'."""
    parts = norm(name).split()
    if not parts:
        return ""
    first   = parts[0].rstrip(".")
    initial = parts[-1][0] if len(parts) > 1 else ""
    return f"{first}_{initial}"


# ── Load datasets ──────────────────────────────────
def load_data():
    profiles = pd.read_csv(PROFILES, dtype=str)
    bikes    = pd.read_csv(BIKES,    dtype=str)

    # All unique names from attendance
    att_names = set()
    try:
        att = pd.read_csv(ATTENDANCE, dtype=str)
        for cell in att["Athletes_Names"].dropna():
            for n in str(cell).split(","):
                n = n.strip()
                if n:
                    att_names.add(n)
    except FileNotFoundError:
        pass

    # All unique names from leaderboard
    lb_names = set()
    try:
        lb = pd.read_csv(LEADERBOARD, dtype=str)
        lb_names = set(lb["Athlete"].dropna().unique())
    except FileNotFoundError:
        pass

    return profiles, bikes, att_names, lb_names


# ── Match logic ────────────────────────────────────
def find_matches(profiles, att_names, lb_names):
    """
    For each followed athlete, try to find them in TNCE data.
    Returns list of match dicts with match_source and match_name.
    """
    # Build lookup dicts for TNCE names
    att_exact  = {norm(n): n for n in att_names}
    att_fuzzy  = {fuzzy_key(n): n for n in att_names}
    lb_exact   = {norm(n): n for n in lb_names}
    lb_fuzzy   = {fuzzy_key(n): n for n in lb_names}

    matches = []
    no_match = []

    for _, row in profiles.iterrows():
        athlete_name = str(row.get("Name", "")).strip()
        if not athlete_name:
            continue

        n     = norm(athlete_name)
        fk    = fuzzy_key(athlete_name)
        found = False

        # Check attendance — exact first
        if n in att_exact:
            matches.append({**row.to_dict(),
                            "Match_Source":  "attendance_exact",
                            "TNCE_Name":     att_exact[n],
                            "Match_Quality": "exact"})
            found = True
        elif fk and fk in att_fuzzy:
            matches.append({**row.to_dict(),
                            "Match_Source":  "attendance_fuzzy",
                            "TNCE_Name":     att_fuzzy[fk],
                            "Match_Quality": "fuzzy"})
            found = True

        # Check leaderboard — exact first
        if not found:
            if n in lb_exact:
                matches.append({**row.to_dict(),
                                "Match_Source":  "leaderboard_exact",
                                "TNCE_Name":     lb_exact[n],
                                "Match_Quality": "exact"})
                found = True
            elif fk and fk in lb_fuzzy:
                matches.append({**row.to_dict(),
                                "Match_Source":  "leaderboard_fuzzy",
                                "TNCE_Name":     lb_fuzzy[fk],
                                "Match_Quality": "fuzzy"})
                found = True

        if not found:
            no_match.append(athlete_name)

    return matches, no_match


# ── Report ─────────────────────────────────────────
def report(matches, no_match, bikes):
    W = 60
    print(f"\n{'='*W}")
    print(f"  STEP 1: FOLLOWED ATHLETES × TNCE CROSS-REFERENCE")
    print(f"{'='*W}")

    total = len(matches) + len(no_match)
    print(f"\n  Followed athletes analysed : {total}")
    print(f"  Matched to TNCE            : {len(matches)}")
    print(f"  Not found in TNCE          : {len(no_match)}")
    if total:
        print(f"  Match rate                 : {len(matches)/total*100:.1f}%")

    if not matches:
        print("\n  No matches found.")
        return pd.DataFrame()

    df = pd.DataFrame(matches)

    # Quality breakdown
    print(f"\n  Match quality:")
    for q, cnt in df["Match_Quality"].value_counts().items():
        print(f"    {q:<10} : {cnt}")

    print(f"\n  Match source:")
    for s, cnt in df["Match_Source"].value_counts().items():
        print(f"    {s:<25} : {cnt}")

    # Numeric conversions
    for col in ["AllTime_km", "CurrYear_km", "Longest_Ride_km",
                "AllTime_acts", "Bike_Count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Matched athletes with their data
    print(f"\n  {'Name':<28} {'TNCE Name':<25} {'AllTime_km':>10} "
          f"{'2026_km':>8} {'Bikes':>6} {'Quality':<8}")
    print(f"  {'─'*28} {'─'*25} {'─'*10} {'─'*8} {'─'*6} {'─'*8}")

    for _, r in df.sort_values("AllTime_km", ascending=False).iterrows():
        print(f"  {str(r['Name'])[:27]:<28} "
              f"{str(r['TNCE_Name'])[:24]:<25} "
              f"{float(r.get('AllTime_km') or 0):>10,.0f} "
              f"{float(r.get('CurrYear_km') or 0):>8,.0f} "
              f"{int(r.get('Bike_Count') or 0):>6} "
              f"{str(r.get('Match_Quality','')):<8}")

    # Bikes for matched athletes
    matched_ids = set(df["Athlete_ID"].astype(str))
    matched_bikes = bikes[bikes["Athlete_ID"].astype(str).isin(matched_ids)]

    if not matched_bikes.empty:
        print(f"\n  Bikes owned by matched TNCE athletes ({len(matched_bikes)} bikes):")
        mb = matched_bikes.merge(
            df[["Athlete_ID", "Name"]].drop_duplicates(),
            on="Athlete_ID", how="left"
        )
        for _, b in mb.iterrows():
            km = float(b.get("Bike_Km") or 0)
            print(f"    {str(b.get('Name',''))[:25]:<25}  "
                  f"{str(b.get('Bike_Name','')):<30}  "
                  f"{km:>8,.0f} km  [{b.get('Brand','')}]")

    # Key insight
    print(f"\n  KEY INSIGHT:")
    print(f"  These {len(df)} athletes are confirmed TNCE members with REAL data.")
    print(f"  Their AllTime_km + bike info can be used as ground truth")
    print(f"  for the ML model without any estimation.")

    return df


# ── Main ───────────────────────────────────────────
def run():
    profiles, bikes, att_names, lb_names = load_data()
    matches, no_match = find_matches(profiles, att_names, lb_names)
    df = report(matches, no_match, bikes)

    if not df.empty:
        df.to_csv(OUT_CSV, index=False)
        print(f"\n  Saved → {OUT_CSV.name}  ({len(df)} rows)")

    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    run()
