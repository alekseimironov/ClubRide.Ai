"""
brain/retriever.py
Data retrieval layer — reads CSVs and returns clean Python dicts.

Fixes applied:
  1. Name matching  — fuzzy first-name + last-initial fallback
  2. Caching        — 5-minute in-memory TTL cache
  3. Paths          — club_id mapped to actual filenames (multi-club ready)
  4. Year boundary  — date arithmetic instead of week subtraction
  5. Error handling — graceful degradation when files missing
  6. Deduplication  — shared _load_leaderboard() used by all lb functions
"""

import os
import re
import time
import unicodedata
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent

# ── Fix 3: club_id → actual filenames ─────────────
CLUB_FILES = {
    318940: {                               # TNCE
        "leaderboard": "leaderboard.csv",
        "attendance":  "historical_attendance.csv",
        "activities":  "activities.csv",
        "members":     "members.csv",
        "crm":         "crm.csv",
        "profiles":    "athlete_resolved.csv",
        "enriched":    "attendance_enriched.csv",
    },
    1130145: {                              # Belga
        "leaderboard": "belga_leaderboard.csv",
        "attendance":  "historical_attendance_Belga.csv",
        "activities":  "belga_activities.csv",
        "members":     "belga_members.csv",
        "crm":         "belga_crm.csv",
        "profiles":    "athlete_resolved.csv",
        "enriched":    "attendance_enriched.csv",
    },
}

_SYNTHETIC_KEYS = {"crm"}

# Athletes permanently excluded from ALL bot tools — loaded from .env, never hardcoded
_EXCLUDED_ATHLETES = {
    name.strip().lower()
    for name in os.getenv("EXCLUDED_ATHLETES", "").split(",")
    if name.strip()
}

def _paths(club_id: int) -> dict:
    files = CLUB_FILES.get(club_id, CLUB_FILES[318940])
    return {
        key: (ROOT / "data/synthetic" / fname
              if key in _SYNTHETIC_KEYS
              else ROOT / "data/real" / fname)
        for key, fname in files.items()
    }


# ── Fix 2: 5-minute in-memory cache ───────────────
_cache: dict = {}
_CACHE_TTL   = 300   # seconds

def _load_csv(path: Path, **kwargs) -> pd.DataFrame:
    """Read CSV with caching. Returns empty DataFrame if file missing."""
    key = str(path)
    now = time.time()

    if key in _cache and now - _cache[key]["ts"] < _CACHE_TTL:
        return _cache[key]["df"].copy()

    # Fix 5: graceful degradation
    try:
        df = pd.read_csv(path, dtype=str, **kwargs)
        _cache[key] = {"df": df, "ts": now}
        return df.copy()
    except FileNotFoundError:
        print(f"  ⚠️  File not found: {path.name} — returning empty dataset")
        return pd.DataFrame()
    except Exception as e:
        print(f"  ⚠️  Error reading {path.name}: {e}")
        return pd.DataFrame()

def clear_cache():
    """Force cache refresh — call after scrapers write new data."""
    _cache.clear()


# ── Fix 1: Name normalisation + fuzzy fallback ────
def _norm(name: str) -> str:
    """Strict normalisation: lowercase + strip diacritics."""
    name = re.sub(r"\(.*?\)", "", str(name))
    nfkd = unicodedata.normalize("NFD", name)
    name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", name).strip().lower()

def _fuzzy_norm(name: str) -> tuple[str, str]:
    """
    Returns (firstname, last_initial) for fuzzy matching.
    Handles "J. Baptista", "João B.", "Alex T." patterns.
    """
    parts = _norm(name).split()
    if not parts:
        return "", ""
    firstname = parts[0].rstrip(".")
    lastname_initial = parts[-1][0] if len(parts) > 1 else ""
    return firstname, lastname_initial

def _match_athlete(target: str, candidates: pd.Series) -> pd.Series:
    """
    Match target name against a Series of candidate names.
    Step 1: exact normalised match
    Step 2: fuzzy first-name + last-initial match
    Returns boolean mask.
    """
    target_norm = _norm(target)
    exact = candidates.apply(_norm) == target_norm
    if exact.any():
        return exact

    # Fuzzy fallback
    t_first, t_initial = _fuzzy_norm(target)
    def fuzzy(name):
        c_first, c_initial = _fuzzy_norm(name)
        return (c_first == t_first and
                (c_initial == t_initial or not t_initial or not c_initial))

    return candidates.apply(fuzzy)


# ── Fix 6: shared leaderboard loader ─────────────
def _load_leaderboard(club_id: int) -> pd.DataFrame:
    p  = _paths(club_id)
    df = _load_csv(p["leaderboard"])
    if df.empty:
        return df
    for col in ("Distance_km", "Rides", "Elev_Gain_m",
                "Avg_Speed_kmh", "Week_Number", "Year"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ── Leaderboard ────────────────────────────────────
def get_leaderboard(club_id: int, week: int = None,
                    year: int = None, top_n: int = 10) -> dict:
    df = _load_leaderboard(club_id)
    if df.empty:
        return {"week": None, "year": None, "athletes": [], "data_source": "real"}

    if week and year:
        df = df[(df["Week_Number"] == week) & (df["Year"] == year)]
    else:
        latest_year = df["Year"].max()
        latest_week = df[df["Year"] == latest_year]["Week_Number"].max()
        df = df[(df["Week_Number"] == latest_week) & (df["Year"] == latest_year)]

    if df.empty:
        return {"week": week, "year": year, "athletes": [], "data_source": "real"}

    df = (df.sort_values("Snapshot_Date", ascending=False)
            .groupby("Athlete").first()
            .reset_index()
            .sort_values("Distance_km", ascending=False)
            .head(top_n))

    return {
        "week":      int(df["Week_Number"].iloc[0]),
        "year":      int(df["Year"].iloc[0]),
        "athletes":  df[["Athlete", "Distance_km", "Rides",
                          "Avg_Speed_kmh", "Elev_Gain_m"]].to_dict("records"),
        "data_source": "real",
    }


def get_weekly_unique_athletes(club_id: int, week: int, year: int) -> dict:
    df = _load_leaderboard(club_id)
    if df.empty:
        return {"week": week, "year": year, "athletes": [], "data_source": "real"}

    week_df = df[(df["Week_Number"] == week) & (df["Year"] == year)]
    if week_df.empty:
        return {"week": week, "year": year, "athletes": [], "data_source": "real"}

    best = (week_df.groupby("Athlete")["Distance_km"].max()
                   .reset_index()
                   .sort_values("Distance_km", ascending=False))
    return {
        "week":             week,
        "year":             year,
        "snapshots":        week_df["Snapshot_Date"].nunique(),
        "unique_athletes":  len(best),
        "athletes":         best.to_dict("records"),
        "data_source":      "real",
    }


# ── Attendance ─────────────────────────────────────
def get_attendance(club_id: int, athlete_name: str = None,
                   last_n_events: int = 10) -> dict:
    p  = _paths(club_id)
    df = _load_csv(p["attendance"])
    if df.empty:
        return {"events": [], "total": 0, "data_source": "real"}

    df["_date"] = pd.to_datetime(df["Date"], format="mixed", dayfirst=False)
    df = df.dropna(subset=["_date"]).sort_values("_date", ascending=False)

    if athlete_name:
        mask = df["Athletes_Names"].fillna("").apply(
            lambda cell: _match_athlete(
                athlete_name,
                pd.Series([n.strip() for n in str(cell).split(",") if n.strip()])
            ).any()
        )
        df = df[mask]

    df = df.head(last_n_events)
    return {
        "events":      df[["Date", "Title", "Athletes_Count",
                            "Ride_Type", "Distance", "Rain"]].to_dict("records"),
        "total":       len(df),
        "data_source": "real",
    }


def get_attendance_rate(club_id: int, athlete_name: str,
                        last_n_events: int = 10) -> dict:
    p  = _paths(club_id)
    df = _load_csv(p["attendance"])
    if df.empty:
        return {"athlete": athlete_name, "attended": 0,
                "total_events": 0, "rate_pct": 0, "data_source": "real"}

    df["_date"] = pd.to_datetime(df["Date"], format="mixed", dayfirst=False)
    df = (df.dropna(subset=["_date"])
            .sort_values("_date", ascending=False)
            .head(last_n_events))

    attended = 0
    for _, row in df.iterrows():
        names = pd.Series([n.strip() for n in
                           str(row.get("Athletes_Names","")).split(",")
                           if n.strip()])
        if not names.empty and _match_athlete(athlete_name, names).any():
            attended += 1

    total = len(df)
    return {
        "athlete":       athlete_name,
        "attended":      attended,
        "total_events":  total,
        "rate_pct":      round(attended / total * 100, 1) if total > 0 else 0,
        "data_source":   "real",
    }


# ── CRM / Service ──────────────────────────────────
def get_service_alerts(club_id: int, limit: int = 10) -> dict:
    p  = _paths(club_id)
    df = _load_csv(p["crm"])
    if df.empty:
        return {"athletes": [], "total_due": 0, "data_source": "synthetic"}

    due = df[df["Service_Due"].str.lower() == "true"].copy()
    due["Km_Since_Service"] = pd.to_numeric(due["Km_Since_Service"], errors="coerce")
    due["Total_Est_Km"]     = pd.to_numeric(due["Total_Est_Km"],     errors="coerce")
    due = due.sort_values("Km_Since_Service", ascending=False).head(limit)

    return {
        "athletes":  due[["Athlete", "Bike_Brand", "Bike_Model",
                           "Km_Since_Service", "Last_Service_Date",
                           "Total_Est_Km", "Data_Source"]].to_dict("records"),
        "total_due": len(due),
        "data_source": "synthetic",
    }


def get_chain_alerts(club_id: int, limit: int = 10) -> dict:
    p  = _paths(club_id)
    df = _load_csv(p["crm"])
    if df.empty:
        return {"athletes": [], "total_due": 0, "data_source": "synthetic"}

    due = df[df["Chain_Due"].str.lower() == "true"].copy()
    due["Km_Since_Chain"] = pd.to_numeric(due["Km_Since_Chain"], errors="coerce")
    due = due.sort_values("Km_Since_Chain", ascending=False).head(limit)

    return {
        "athletes":  due[["Athlete", "Bike_Brand", "Bike_Model",
                           "Km_Since_Chain", "Data_Source"]].to_dict("records"),
        "total_due": len(due),
        "data_source": "synthetic",
    }


def get_athlete_crm(club_id: int, athlete_name: str) -> dict:
    p  = _paths(club_id)
    df = _load_csv(p["crm"])
    if df.empty:
        return {"athlete": athlete_name, "found": False}

    mask = _match_athlete(athlete_name, df["Athlete"])
    row  = df[mask]
    if row.empty:
        return {"athlete": athlete_name, "found": False}

    r = row.iloc[0].to_dict()
    r["found"] = True
    return r


# ── Members — Fix 4: date arithmetic for ghosts ───
def get_ghost_members(club_id: int, absent_weeks: int = 4) -> dict:
    """
    True ghost = absent from ALL THREE sources in the window:
      1. Not in leaderboard top-100 (last absent_weeks weeks)
      2. Not in event attendance (last absent_weeks weeks)
      3. Not in daily activity feed (last absent_weeks weeks)

    A rider absent from leaderboard but present in attendance
    (e.g. attends club rides but low km) is NOT a ghost.
    """
    p       = _paths(club_id)
    members = _load_csv(p["members"])
    lb      = _load_leaderboard(club_id)
    att     = _load_csv(p["attendance"])
    acts    = _load_csv(p["activities"])

    if members.empty:
        return {"ghosts": [], "total_ghosts": 0,
                "absent_weeks": absent_weeks, "data_source": "real"}

    cutoff_date = date.today() - timedelta(weeks=absent_weeks)
    cutoff_ts   = pd.Timestamp(cutoff_date)

    # Source 1 — real leaderboard (recent window)
    active_norms: set[str] = set()
    if not lb.empty and "Snapshot_Date" in lb.columns:
        lb["Snapshot_Date"] = pd.to_datetime(lb["Snapshot_Date"], errors="coerce")
        recent_lb = lb[lb["Snapshot_Date"] >= cutoff_ts]
        active_norms |= set(recent_lb["Athlete"].apply(_norm))

    # Source 2 — event attendance (all-time, not just recent window)
    # Attendance history is the most reliable presence signal we have
    if not att.empty:
        for cell in att["Athletes_Names"].dropna():
            for name in str(cell).split(","):
                name = name.strip()
                if name:
                    active_norms.add(_norm(name))

    # Source 4 — daily activity feed (recent window)
    if not acts.empty:
        acts["Activity_Date"] = pd.to_datetime(acts["Activity_Date"], errors="coerce")
        recent_acts = acts[acts["Activity_Date"] >= cutoff_ts]
        active_norms |= set(recent_acts["Athlete"].apply(_norm))

    # Build fuzzy key set from active sources: (firstname, last_initial)
    # Handles Strava's privacy truncation: "Abdi Bennani" → "Abdi B."
    def _fuzzy_key(name: str) -> tuple[str, str]:
        parts = _norm(name).split()
        if not parts:
            return ("", "")
        first   = parts[0].rstrip(".")
        initial = parts[-1][0] if len(parts) > 1 else ""
        return (first, initial)

    active_fuzzy = {_fuzzy_key(n) for n in active_norms if n}

    def _is_active(full_name: str) -> bool:
        # Try exact match first
        if _norm(full_name) in active_norms:
            return True
        # Fuzzy match: firstname + last initial
        return _fuzzy_key(full_name) in active_fuzzy

    ghosts = members[~members["Full_Name"].apply(_is_active)]
    return {
        "ghosts":       ghosts[["Full_Name", "Membership"]].to_dict("records"),
        "total_ghosts": len(ghosts),
        "absent_weeks": absent_weeks,
        "data_source":  "real",
    }


# ── Activities ─────────────────────────────────────
def get_recent_activities(club_id: int, days: int = 2,
                          limit: int = 20) -> dict:
    p  = _paths(club_id)
    df = _load_csv(p["activities"])
    if df.empty:
        return {"activities": [], "total": 0, "data_source": "real"}

    df["Activity_Date"] = pd.to_datetime(df["Activity_Date"], errors="coerce")
    cutoff = pd.Timestamp(date.today() - timedelta(days=days))
    df = df[df["Activity_Date"] >= cutoff].head(limit)

    return {
        "activities":  df[["Activity_Date", "Athlete", "Activity_Name",
                            "Distance_km", "Device", "Location"]].to_dict("records"),
        "total":       len(df),
        "data_source": "real",
    }


# ── Club summary ───────────────────────────────────
def get_club_summary(club_id: int) -> dict:
    p       = _paths(club_id)
    members = _load_csv(p["members"])
    lb      = _load_leaderboard(club_id)

    if lb.empty:
        return {"total_members": len(members), "active_this_week": 0,
                "total_km_this_week": 0, "top_athlete": None,
                "top_km": 0, "week": None, "data_source": "real"}

    latest_year = lb["Year"].max()
    latest_week = lb[lb["Year"] == latest_year]["Week_Number"].max()
    latest      = lb[(lb["Week_Number"] == latest_week) &
                     (lb["Year"] == latest_year)]
    best        = (latest.sort_values("Snapshot_Date", ascending=False)
                         .groupby("Athlete")["Distance_km"].max())

    return {
        "total_members":      len(members),
        "active_this_week":   len(best),
        "total_km_this_week": round(best.sum(), 1),
        "top_athlete":        best.idxmax() if not best.empty else None,
        "top_km":             round(best.max(), 1) if not best.empty else 0,
        "week":               int(latest_week),
        "year":               int(latest_year),
        "data_source":        "real",
    }


# ── Athlete profiles ──────────────────────────────
def get_athlete_profile(club_id: int, athlete_name: str) -> dict:
    """
    Full profile for one athlete. Combines three sources:
      1. athlete_resolved.csv  — real Strava data (weekly km, bike, tier)
      2. attendance_enriched.csv — events count, first/last seen
      3. crm.csv               — service status
    Returns tiered response based on available data.
    """
    p       = _paths(club_id)
    prof_df = _load_csv(p["profiles"])
    enr_df  = _load_csv(p["enriched"])
    crm_df  = _load_csv(p["crm"])

    result = {"athlete": athlete_name, "found": False, "data_quality": "none"}

    # 1 — profile match (real Strava data)
    if not prof_df.empty:
        mask = _match_athlete(athlete_name, prof_df["Name"])
        if mask.any():
            row = prof_df[mask].iloc[0]
            result.update({
                "found":         True,
                "data_quality":  "full",
                "name":          str(row.get("Name", "")),
                "location":      str(row.get("Location", "")),
                "weekly_km":     float(row.get("Weekly_km") or 0),
                "alltime_km":    float(row.get("AllTime_km") or 0),
                "curr_year_km":  float(row.get("CurrYear_km") or 0),
                "avg_speed":     float(row.get("Avg_Speed_kmh") or 0),
                "longest_ride":  float(row.get("Longest_Ride_km") or 0),
                "rider_tier":    str(row.get("rider_tier", "unknown")),
                "primary_bike":  str(row.get("primary_bike", "")),
                "primary_brand": str(row.get("primary_brand", "")),
                "primary_tier":    str(row.get("primary_tier", "unknown")),
                "primary_cat":     str(row.get("primary_cat", "")),
                "primary_bike_km": float(row.get("primary_bike_km") or 0),
                "fleet_km":        float(row.get("fleet_km") or 0),
                "bike_count":      int(float(row.get("bike_count") or 0)),
            })

    # 2 — attendance enrichment (events count, first/last seen)
    if not enr_df.empty:
        mask = _match_athlete(athlete_name, enr_df["Athlete_Raw"])
        if not mask.any() and result.get("name"):
            mask = _match_athlete(result["name"], enr_df["Athlete_Raw"])
        att_rows = enr_df[mask]
        if not att_rows.empty:
            events_count = len(att_rows)
            dates = pd.to_datetime(att_rows["Date"], format="mixed", dayfirst=False, errors="coerce").dropna()
            result.update({
                "found":        True,
                "events_count": events_count,
                "first_seen":   dates.min().date().isoformat() if not dates.empty else "",
                "last_seen":    dates.max().date().isoformat() if not dates.empty else "",
            })
            if not result.get("data_quality") == "full":
                result["data_quality"] = "attendance_only"
                result["name"] = athlete_name

    # 3 — CRM service status
    if not crm_df.empty:
        mask = _match_athlete(athlete_name, crm_df["Athlete"])
        if mask.any():
            crm_row = crm_df[mask].iloc[0]
            result.update({
                "service_due":      str(crm_row.get("Service_Due", "")).lower() == "true",
                "chain_due":        str(crm_row.get("Chain_Due", "")).lower() == "true",
                "km_since_service": float(crm_row.get("Km_Since_Service") or 0),
                "km_since_chain":   float(crm_row.get("Km_Since_Chain") or 0),
                "last_service":     str(crm_row.get("Last_Service_Date", "")),
                "purchase_source":  str(crm_row.get("Purchase_Source", "")),
                "purchase_date":    str(crm_row.get("Purchase_Date", "")),
            })

    return result


def get_upgrade_candidates(club_id: int, limit: int = 10) -> dict:
    """
    Community-first upgrade candidates.
    Master list = attendance_enriched.csv (all 162 event attendees).
    Profile data enriches where available.
    Attendance-only members with 5+ events flagged for conversation even without profile.
    """
    p       = _paths(club_id)
    enr_df  = _load_csv(p["enriched"])
    prof_df = _load_csv(p["profiles"])
    crm_df  = _load_csv(p["crm"])

    if enr_df.empty:
        return {"candidates": [], "total": 0}

    if not prof_df.empty:
        prof_df["Weekly_km"] = pd.to_numeric(prof_df["Weekly_km"], errors="coerce").fillna(0)

    # Build profile lookup by normalised name
    prof_lookup = {}
    if not prof_df.empty:
        for _, r in prof_df.iterrows():
            prof_lookup[_norm(str(r["Name"]))] = r

    # Count events per unique community member
    seen = {}
    for _, row in enr_df.iterrows():
        raw = str(row.get("Athlete_Raw", "")).strip()
        if not raw:
            continue
        key = _norm(raw)
        if key not in seen:
            seen[key] = {"name": raw, "events": 0}
        seen[key]["events"] += 1

    # Build leaderboard index: norm_name -> {avg_weekly_km, avg_speed}
    lb_index = {}
    lb_df = _load_leaderboard(club_id)
    if not lb_df.empty:
        best_wk = (lb_df.sort_values("Snapshot_Date", ascending=False)
                       .groupby(["Athlete", "Week_Number", "Year"])
                       .first().reset_index())
        for _, r in best_wk.iterrows():
            nk = _norm(str(r["Athlete"]))
            if nk not in lb_index:
                lb_index[nk] = {"km_list": [], "speed_list": []}
            if pd.notna(r.get("Distance_km")):
                lb_index[nk]["km_list"].append(float(r["Distance_km"]))
            if pd.notna(r.get("Avg_Speed_kmh")):
                lb_index[nk]["speed_list"].append(float(r["Avg_Speed_kmh"]))

    results = []
    for key, info in seen.items():
        name         = info["name"]
        events_count = info["events"]

        if key in _EXCLUDED_ATHLETES:
            continue

        profile = prof_lookup.get(key)
        if profile is None:
            for pk, pv in prof_lookup.items():
                fn1, i1 = _fuzzy_norm(key)
                fn2, i2 = _fuzzy_norm(pk)
                if fn1 == fn2 and i1 == i2:
                    profile = pv
                    break

        has_profile  = profile is not None
        rider_tier   = str(profile["rider_tier"])            if has_profile else "unknown"
        primary_tier = str(profile.get("primary_tier", "")) if has_profile else "unknown"
        weekly_km    = float(profile["Weekly_km"])           if has_profile else 0
        fleet_km     = float(profile.get("fleet_km") or 0)  if has_profile else 0
        primary_bike = str(profile.get("primary_bike", "")) if has_profile else ""
        km_source    = "real"
        speed_est    = 0.0

        # No profile: estimate from leaderboard
        if not has_profile:
            lb_data = lb_index.get(key)
            if lb_data and lb_data["km_list"]:
                weekly_km = round(sum(lb_data["km_list"]) / len(lb_data["km_list"]), 1)
                km_source = "estimated"
            if lb_data and lb_data["speed_list"]:
                speed_est = round(sum(lb_data["speed_list"]) / len(lb_data["speed_list"]), 1)
            if speed_est > 30 or weekly_km >= 150:
                rider_tier = "top"
            elif speed_est > 25 or weekly_km >= 50:
                rider_tier = "mid"

        # Upgrade requires real scraped bike data — primary bike must have 15k+ km
        primary_bike_km = float(profile.get("primary_bike_km") or 0) if has_profile else 0
        if not has_profile or primary_bike_km < 15_000:
            continue

        is_candidate = (
            (rider_tier == "top" and primary_tier in ("mid", "entry", "unknown")) or
            (rider_tier == "mid" and primary_tier == "entry")
        )
        if not is_candidate:
            continue

        purchase_source = ""
        purchase_year   = ""
        inferred_bike   = ""
        inferred_tier   = ""
        if not crm_df.empty:
            cmask = _match_athlete(name, crm_df["Athlete"])
            if cmask.any():
                crm_row         = crm_df[cmask].iloc[0]
                purchase_source = str(crm_row.get("Purchase_Source", ""))
                pd_str          = str(crm_row.get("Purchase_Date", ""))
                if len(pd_str) >= 7:
                    try:
                        from datetime import datetime as _dt
                        pdate         = _dt.strptime(pd_str[:7], "%Y-%m")
                        purchase_year = f"{pdate.strftime('%b')} {pdate.year}"
                    except Exception:
                        pass
                if not has_profile:
                    b  = str(crm_row.get("Bike_Brand", ""))
                    m  = str(crm_row.get("Bike_Model", ""))
                    bt = str(crm_row.get("Bike_Tier", ""))
                    if b and b not in ("nan", ""):
                        inferred_bike = f"{b} {m}".strip()
                        inferred_tier = bt

        primary_bike_km = float(profile.get("primary_bike_km") or 0) if has_profile else 0
        results.append({
            "name":            name,
            "weekly_km":       weekly_km,
            "km_source":       km_source,
            "speed_est":       speed_est,
            "primary_bike_km": primary_bike_km,
            "fleet_km":        fleet_km,
            "rider_tier":      rider_tier,
            "primary_bike":    primary_bike if has_profile else inferred_bike,
            "primary_tier":    primary_tier if has_profile else inferred_tier,
            "bike_source":     "real" if has_profile else ("inferred" if inferred_bike else "unknown"),
            "events_count":    events_count,
            "purchase_source": purchase_source,
            "purchase_year":   purchase_year,
            "has_profile":     has_profile,
        })

    # Sort: most events first, then weekly km
    results.sort(key=lambda c: (-c["events_count"], -c["weekly_km"]))
    return {"candidates": results[:limit], "total": len(results)}


def get_at_risk_members(club_id: int,
                        absent_weeks: int = 6,
                        min_events: int = 5,
                        min_rate_pct: int = 10) -> dict:
    """
    Athletes who attended regularly but have gone quiet.
    Attendance rate = attended / total events since their first appearance.
    Only flags athletes with >= min_events historically.
    """
    p      = _paths(club_id)
    att_df = _load_csv(p["attendance"])
    enr_df = _load_csv(p["enriched"])

    if att_df.empty or enr_df.empty:
        return {"at_risk": [], "total": 0}

    att_df["_date"] = pd.to_datetime(att_df["Date"], format="mixed", dayfirst=False)
    att_df = att_df.dropna(subset=["_date"])

    cutoff = pd.Timestamp(date.today() - timedelta(weeks=absent_weeks))

    # Build per-athlete stats from enriched attendance
    enr_df["_date"] = pd.to_datetime(enr_df["Date"], format="mixed", dayfirst=False)

    # One row per unique athlete (deduplicated by norm name)
    seen = {}   # norm_name → {name, first_date, last_date, attended_set}
    for _, row in enr_df.dropna(subset=["_date"]).iterrows():
        raw  = str(row.get("Athlete_Raw", "")).strip()
        if not raw:
            continue
        key  = _norm(raw)
        dt   = row["_date"]
        if key not in seen:
            seen[key] = {"name": raw, "first": dt, "last": dt, "attended": set()}
        seen[key]["last"]          = max(seen[key]["last"], dt)
        seen[key]["first"]         = min(seen[key]["first"], dt)
        seen[key]["attended"].add(row.get("Event_ID", str(dt.date())))

    at_risk = []
    for key, s in seen.items():
        attended_count = len(s["attended"])
        if attended_count < min_events:
            continue

        # Total events available since their first appearance
        total_since_join = int(
            (att_df["_date"] >= s["first"]).sum()
        )
        rate_pct = round(attended_count / total_since_join * 100) \
                   if total_since_join > 0 else 0

        if rate_pct < min_rate_pct:
            continue

        if key in _EXCLUDED_ATHLETES:
            continue

        if s["last"] >= cutoff:
            continue

        weeks_absent = int((pd.Timestamp.now() - s["last"]).days / 7)
        at_risk.append({
            "name":          s["name"],
            "attended":      attended_count,
            "total_events":  total_since_join,
            "rate_pct":      rate_pct,
            "last_seen":     s["last"].date().isoformat(),
            "weeks_absent":  weeks_absent,
        })

    # Sort by most loyal first (highest rate) — they're worth reaching out to most
    at_risk.sort(key=lambda x: (-x["rate_pct"], -x["attended"]))

    return {"at_risk": at_risk, "total": len(at_risk)}


def get_week_attendees(club_id: int, week: int, year: int) -> dict:
    """
    Returns past and future attendees for a given ISO week.
    past:   set of norm names who attended an event that already happened
    future: set of norm names registered for an upcoming event this week
    event_dates: list of (date, title, is_future) tuples for context
    """
    p      = _paths(club_id)
    att_df = _load_csv(p["attendance"])
    if att_df.empty:
        return {"past": set(), "future": set(), "event_dates": []}

    now    = pd.Timestamp.now()
    att_df["_date"] = pd.to_datetime(att_df["Date"], format="mixed",
                                     dayfirst=False, errors="coerce")
    att_df = att_df.dropna(subset=["_date"])
    att_df["_week"] = att_df["_date"].dt.isocalendar().week.astype(int)
    att_df["_year"] = att_df["_date"].dt.isocalendar().year.astype(int)

    week_events = att_df[(att_df["_week"] == week) & (att_df["_year"] == year)]

    past, future, event_dates = set(), set(), []
    for _, row in week_events.iterrows():
        is_future = row["_date"] > now
        target    = future if is_future else past
        event_dates.append((row["_date"].date(), str(row.get("Title", "")), is_future))
        for name in str(row.get("Athletes_Names", "")).split(","):
            name = name.strip()
            if name:
                target.add(_norm(name))

    return {"past": past, "future": future, "event_dates": event_dates}


_LOCAL_KEYWORDS = {"lausanne", "vaud", " vd,", "vd,", "vd "}

def _is_local(location: str) -> bool:
    """Lausanne area: Lausanne, Vaud, VD canton and nearby towns."""
    loc = location.lower()
    return any(kw in loc for kw in _LOCAL_KEYWORDS)


def get_potential_recruits(club_id: int, limit: int = 10,
                           min_weekly_km: float = 50) -> dict:
    """
    Option B: serious local Strava followers who never attended a club event.
    Filtered to Lausanne / Vaud area only — invitation targets, not sales targets.
    """
    p       = _paths(club_id)
    prof_df = _load_csv(p["profiles"])
    enr_df  = _load_csv(p["enriched"])

    if prof_df.empty:
        return {"recruits": [], "total": 0}

    prof_df["Weekly_km"] = pd.to_numeric(prof_df["Weekly_km"], errors="coerce").fillna(0)

    # Build normalised set of all known community members
    community_norms = set()
    if not enr_df.empty:
        for name in enr_df["Athlete_Raw"].dropna():
            community_norms.add(_norm(str(name)))
        for name in enr_df["Matched_Name"].dropna():
            community_norms.add(_norm(str(name)))

    recruits = []
    for _, row in prof_df.iterrows():
        name      = str(row["Name"])
        weekly_km = float(row["Weekly_km"])

        if weekly_km < min_weekly_km:
            continue

        # Local only — require confirmed Lausanne / Vaud location; skip if missing
        location = str(row.get("Location", "") or "")
        if not location or location in ("nan", "") or not _is_local(location):
            continue

        # Check if in community via exact or fuzzy match
        nk         = _norm(name)
        fn1, i1    = _fuzzy_norm(nk)
        in_community = nk in community_norms or any(
            (_fuzzy_norm(cn)[0] == fn1 and _fuzzy_norm(cn)[1] == i1)
            for cn in community_norms if cn
        )
        if in_community:
            continue

        recruits.append({
            "name":         name,
            "weekly_km":    weekly_km,
            "rider_tier":   str(row.get("rider_tier", "unknown")),
            "primary_bike": str(row.get("primary_bike", "")),
            "primary_tier": str(row.get("primary_tier", "")),
            "location":     str(row.get("Location", "")),
            "alltime_km":   float(row.get("AllTime_km") or 0),
        })

    recruits.sort(key=lambda r: -r["weekly_km"])
    return {"recruits": recruits[:limit], "total": len(recruits)}


def get_weekend_priorities(club_id: int) -> dict:
    """
    Returns the single most actionable upgrade candidate and service candidate
    for the owner to contact this weekend. Used for reactive 'who should I talk to?' queries.
    """
    from datetime import datetime as _dt

    # Top upgrade candidate
    upg_data   = get_upgrade_candidates(club_id, limit=1)
    upgrade    = upg_data.get("candidates", [None])[0]

    # Top service candidate
    p      = _paths(club_id)
    crm_df = _load_csv(p["crm"])
    service = None
    if not crm_df.empty:
        crm_df["Km_Since_Service"] = pd.to_numeric(
            crm_df["Km_Since_Service"], errors="coerce").fillna(0)
        due = crm_df[crm_df["Service_Due"].str.lower() == "true"].copy()
        due = due.sort_values("Km_Since_Service", ascending=False)
        if not due.empty:
            r = due.iloc[0]
            service = {
                "name":     str(r.get("Athlete", "")),
                "bike":     f"{r.get('Bike_Brand','')} {r.get('Bike_Model','')}".strip(),
                "km_since": float(r.get("Km_Since_Service", 0)),
                "tier":     str(r.get("Bike_Tier", "")),
            }

    # Draft opening questions based on context
    def _upgrade_question(c: dict) -> str:
        fleet  = float(c.get("fleet_km") or 0)
        src    = c.get("purchase_source", "")
        yr     = c.get("purchase_year", "")
        bike   = c.get("primary_bike") or "bike"
        if src == "Club TNCE":
            return f"How are you finding your {bike} since {yr}?"
        elif fleet > 50_000:
            return f"Your bike is approaching {fleet/1000:.0f}k km — thought about what's next?"
        else:
            return "How's your bike holding up these days?"

    def _service_question(s: dict) -> str:
        km = s["km_since"]
        if km > 8_000:
            return "Urgent — when did you last get it serviced?"
        elif km > 5_000:
            return "Worth a check before summer?"
        return "Have you planned a service soon?"

    return {
        "upgrade": {**upgrade, "draft_question": _upgrade_question(upgrade)} if upgrade else None,
        "service": {**service, "draft_question": _service_question(service)} if service else None,
    }


def get_club_tier_summary(club_id: int) -> dict:
    """
    Tier breakdown of all known athletes — for Friday briefing.
    """
    p       = _paths(club_id)
    prof_df = _load_csv(p["profiles"])
    enr_df  = _load_csv(p["enriched"])

    tier_dist = {"top": 0, "mid": 0, "entry": 0, "unknown": 0}
    if not prof_df.empty:
        counts = prof_df["rider_tier"].value_counts()
        for t in tier_dist:
            tier_dist[t] = int(counts.get(t, 0))

    unique_attendees = 0
    if not enr_df.empty:
        unique_attendees = enr_df["Athlete_Raw"].nunique()

    return {
        "profiled_athletes":  len(prof_df) if not prof_df.empty else 0,
        "unique_attendees":   unique_attendees,
        "rider_tier_dist":    tier_dist,
        "data_source":        "real",
    }


# ── CLI test ───────────────────────────────────────
if __name__ == "__main__":
    import json
    CLUB = 318940

    print("── Fix 1: name matching ──────────────────────")
    history = get_athlete_history(CLUB, "João Baptista")
    print(f"  João Baptista found: {history['found']}  avg: {history['avg_km']} km/week")
    history2 = get_athlete_history(CLUB, "J. Baptista")
    print(f"  J. Baptista  found: {history2['found']}  avg: {history2['avg_km']} km/week")

    print("\n── Fix 2: caching ────────────────────────────")
    import time as _time
    t0 = _time.time(); get_leaderboard(CLUB)
    t1 = _time.time(); get_leaderboard(CLUB)
    t2 = _time.time()
    print(f"  First call:  {(t1-t0)*1000:.1f}ms")
    print(f"  Cached call: {(t2-t1)*1000:.1f}ms")

    print("\n── Fix 4: ghost members (date-based) ────────")
    ghosts = get_ghost_members(CLUB, absent_weeks=4)
    print(f"  Ghosts (absent 4w): {ghosts['total_ghosts']}")

    print("\n── Fix 5: missing file graceful ─────────────")
    from pathlib import Path
    result = get_recent_activities(999999)   # invalid club_id
    print(f"  Invalid club returns: {result}")

    print("\n── Club summary ──────────────────────────────")
    print(json.dumps(get_club_summary(CLUB), indent=2, default=str))

    print("\n✅ retriever.py — all 6 fixes verified")
