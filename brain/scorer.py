"""
brain/scorer.py
Deterministic signal engine — computes 4 signals per athlete.
Zero Gemini calls. Pure formula + logic.

Signals computed:
  upgrade_score    (0–100) — who is ready for an upgrade conversation
  service_flag     (bool)  — km_since_service >= threshold, not suppressed
  chain_flag       (bool)  — km_since_chain >= threshold, not suppressed
  ghost_flag       (bool)  — member with zero recent activity
  data_confidence  (str)   — real / mixed / synthetic

Formula (upgrade_score):
  mileage_score    × 0.40
  bike_age_score   × 0.25
  bike_tier_score  × 0.20
  attendance_score × 0.10
  trend_score      × 0.05
"""

from datetime import date, datetime
from pathlib import Path

from .retriever import (
    get_attendance_rate,
    get_athlete_crm,
    get_service_alerts,
    get_chain_alerts,
    get_ghost_members,
    get_weekly_unique_athletes,
)
from .feedback import (
    pending_alerts,
    suppressed_athletes,
)

# ── Bike tier classification ───────────────────────
# Maps model keywords → tier (entry / mid / top)
BIKE_TIERS = {
    "top": [
        "dogma", "r5", "s5", "madone slr", "tarmac slr",
        "addict rc pro", "teammachine slr01", "795 blade rs",
        "propel advanced sl", "foil rc", "caledonia 5",
        "supersix evo hi-mod",
    ],
    "mid": [
        "tarmac sl6", "tarmac sl7", "emonda sl", "domane sl",
        "addict rc", "speedster", "tcr advanced", "defy advanced",
        "teammachine slr02", "roadmachine", "785 huez", "765 optimum",
        "orca m20", "scultura 6000", "reacto 5000", "supersix evo",
        "synapse carbon", "caledonia",
    ],
}

def _bike_tier(brand: str, model: str) -> str:
    key = f"{brand} {model}".lower()
    for tier, keywords in BIKE_TIERS.items():
        if any(kw in key for kw in keywords):
            return tier
    return "entry"   # default — entry if not recognised


# ── Sub-score functions (each returns 0.0–1.0) ────

def _mileage_score(avg_km: float) -> float:
    if avg_km < 50:   return 0.1
    if avg_km < 150:  return 0.3
    if avg_km < 300:  return 0.6
    if avg_km < 500:  return 0.8
    return 1.0

def _bike_age_score(purchase_date_str: str) -> float:
    """
    ≤ 2 years → 0.0  (new — don't pitch)
    3–4 years → 0.5  (OK — watch)
    ≥ 5 years → 1.0  (upgrade time)
    """
    try:
        purchase = date.fromisoformat(str(purchase_date_str))
        years    = (date.today() - purchase).days / 365.25
    except (ValueError, TypeError):
        return 0.3   # unknown age → neutral

    if years <= 2:  return 0.0
    if years <= 4:  return 0.5
    return 1.0

def _bike_tier_score(tier: str) -> float:
    return {"entry": 1.0, "mid": 0.5, "top": 0.0}.get(tier, 1.0)

def _attendance_score(rate_pct: float) -> float:
    if rate_pct < 20:  return 0.2
    if rate_pct < 50:  return 0.5
    if rate_pct < 80:  return 0.8
    return 1.0

def _trend_score(history: list[dict]) -> float:
    """
    Compare avg km of last 2 weeks vs prior 2 weeks.
    increasing → 1.0 / stable → 0.5 / decreasing → 0.0
    """
    if len(history) < 4:
        return 0.5   # not enough data → neutral

    recent = [h["Distance_km"] for h in history[:2]
              if h.get("Distance_km") is not None]
    prior  = [h["Distance_km"] for h in history[2:4]
              if h.get("Distance_km") is not None]

    if not recent or not prior:
        return 0.5

    avg_recent = sum(recent) / len(recent)
    avg_prior  = sum(prior)  / len(prior)

    if avg_prior == 0:
        return 0.5

    change = (avg_recent - avg_prior) / avg_prior
    if change > 0.10:   return 1.0   # +10% or more = increasing
    if change < -0.10:  return 0.0   # -10% or more = decreasing
    return 0.5                        # within ±10% = stable


# ── Upgrade score ──────────────────────────────────
def compute_upgrade_score(avg_km: float, purchase_date: str,
                           bike_brand: str, bike_model: str,
                           attendance_rate: float,
                           history: list[dict]) -> tuple[int, list[str]]:
    """
    Returns (score 0-100, list of reason codes).
    Reason codes explain WHY the score is high — passed to Gemini.
    """
    tier = _bike_tier(bike_brand, bike_model)

    m = _mileage_score(avg_km)
    a = _bike_age_score(purchase_date)
    t = _bike_tier_score(tier)
    r = _attendance_score(attendance_rate)
    g = _trend_score(history)

    raw   = m * 0.40 + a * 0.25 + t * 0.20 + r * 0.10 + g * 0.05
    score = min(100, round(raw * 100))

    # Build human-readable reason codes
    reasons = []
    if m >= 0.8:   reasons.append("high_mileage")
    if a >= 0.5:   reasons.append("bike_ageing")
    if a == 1.0:   reasons.append("bike_overdue")
    if t == 1.0:   reasons.append("entry_bike")
    if t == 0.5:   reasons.append("mid_bike")
    if r >= 0.8:   reasons.append("loyal_member")
    if g == 1.0:   reasons.append("growing_rider")
    if g == 0.0:   reasons.append("declining_activity")

    return score, reasons


# ── Data confidence ────────────────────────────────
def compute_confidence(history: list[dict]) -> str:
    """
    real     → all history rows are real leaderboard data
    mixed    → some real + some synthetic
    synthetic→ no real data at all
    """
    if not history:
        return "synthetic"
    sources = {h.get("Data_Source", "synthetic") for h in history}
    if sources == {"real"}:
        return "real"
    if "real" in sources:
        return "mixed"
    return "synthetic"


# ── Full athlete profile ───────────────────────────
def score_athlete(club_id: int, athlete_name: str) -> dict:
    """
    Compute all 4 signals for one athlete.
    Single entry point used by prompter and briefing builder.
    """
    # ── Retrieve data ──────────────────────────────
    attendance   = get_attendance_rate(club_id, athlete_name, last_n_events=10)
    crm          = get_athlete_crm(club_id, athlete_name)

    avg_km       = 0
    hist_rows    = []
    att_rate     = attendance.get("rate_pct", 0)

    # ── Service / chain flags ──────────────────────
    svc_suppressed   = suppressed_athletes("service_due")
    chain_suppressed = suppressed_athletes("chain_due")
    name_lower       = athlete_name.strip().lower()

    service_flag  = False
    service_urgent= False
    chain_flag    = False

    if crm.get("found"):
        km_svc   = float(crm.get("Km_Since_Service", 0) or 0)
        km_chain = float(crm.get("Km_Since_Chain",   0) or 0)

        if name_lower not in svc_suppressed:
            service_flag   = crm.get("Service_Due",  "false").lower() == "true"
            service_urgent = km_svc >= 7500

        if name_lower not in chain_suppressed:
            chain_flag = crm.get("Chain_Due", "false").lower() == "true"

    # ── Upgrade score ──────────────────────────────
    if crm.get("found"):
        upgrade_score, upgrade_reasons = compute_upgrade_score(
            avg_km        = avg_km,
            purchase_date = crm.get("Purchase_Date", ""),
            bike_brand    = crm.get("Bike_Brand",    ""),
            bike_model    = crm.get("Bike_Model",    ""),
            attendance_rate = att_rate,
            history       = hist_rows,
        )
        bike_tier = _bike_tier(crm.get("Bike_Brand",""), crm.get("Bike_Model",""))
        try:
            bike_age = round(
                (date.today() - date.fromisoformat(crm.get("Purchase_Date",""))).days / 365.25, 1
            )
        except (ValueError, TypeError):
            bike_age = None
    else:
        upgrade_score   = 0
        upgrade_reasons = []
        bike_tier       = "unknown"
        bike_age        = None

    # ── Ghost flag ─────────────────────────────────
    # Simple check: no history AND no attendance
    ghost_flag = (not hist_rows and att_rate == 0)

    # ── Data confidence ────────────────────────────
    confidence = compute_confidence(hist_rows)

    return {
        "athlete":          athlete_name,
        "avg_km_week":      avg_km,
        "attendance_rate":  att_rate,
        "km_trend":         _trend_label(hist_rows),
        "upgrade_score":    upgrade_score,
        "upgrade_reasons":  upgrade_reasons,
        "bike_brand":       crm.get("Bike_Brand", ""),
        "bike_model":       crm.get("Bike_Model", ""),
        "bike_tier":        bike_tier,
        "bike_age_years":   bike_age,
        "service_flag":     service_flag,
        "service_urgent":   service_urgent,
        "chain_flag":       chain_flag,
        "ghost_flag":       ghost_flag,
        "data_confidence":  confidence,
    }


def _trend_label(history: list[dict]) -> str:
    s = _trend_score(history)
    return {1.0: "increasing", 0.5: "stable", 0.0: "decreasing"}.get(s, "stable")


# ── Batch scoring ──────────────────────────────────
def get_upgrade_candidates(club_id: int,
                           min_score: int = 60,
                           limit: int = 10) -> list[dict]:
    """
    Score all athletes from latest leaderboard week.
    Returns those above min_score, sorted by score desc.
    Used for Friday briefing.
    """
    from .retriever import get_leaderboard
    lb       = get_leaderboard(club_id, top_n=50)
    athletes = [a["Athlete"] for a in lb.get("athletes", [])]

    results = []
    for name in athletes:
        profile = score_athlete(club_id, name)
        if profile["upgrade_score"] >= min_score:
            results.append(profile)

    return sorted(results, key=lambda x: x["upgrade_score"], reverse=True)[:limit]


def get_service_due(club_id: int, limit: int = 10) -> list[dict]:
    """
    Service alerts not suppressed by feedback.
    Returns athletes sorted by km_since_service desc.
    """
    alerts   = get_service_alerts(club_id, limit=limit * 2)
    athletes = [a["Athlete"] for a in alerts.get("athletes", [])]
    pending  = pending_alerts("service_due", athletes)
    return [a for a in alerts["athletes"] if a["Athlete"] in pending][:limit]


def get_ghosts(club_id: int) -> list[dict]:
    """Ghost members — not suppressed."""
    result   = get_ghost_members(club_id, absent_weeks=4)
    ghosts   = [g["Full_Name"] for g in result.get("ghosts", [])]
    pending  = pending_alerts("ghost", ghosts)
    return [g for g in result["ghosts"] if g["Full_Name"] in pending]


# ── CLI test ───────────────────────────────────────
if __name__ == "__main__":
    import json
    CLUB = 318940

    print("── Upgrade score formula test ────────────────")
    cases = [
        ("Elite rider",  600, "2020-01-01", "Specialized", "Allez Sprint",  80),
        ("Mid rider",    200, "2023-06-01", "Trek",        "Emonda SL",     50),
        ("Casual rider",  60, "2024-03-01", "Giant",       "Defy Advanced", 20),
        ("Top bike",     500, "2021-01-01", "Pinarello",   "Dogma F",       70),
    ]
    for label, km, purchase, brand, model, att in cases:
        score, reasons = compute_upgrade_score(km, purchase, brand, model, att, [])
        tier = _bike_tier(brand, model)
        print(f"  {label:15} | {km:4}km/wk | {tier:6} | "
              f"age={_bike_age_score(purchase):.1f} | "
              f"score={score:3}/100 | {reasons}")

    print("\n── Full athlete profile ──────────────────────")
    profile = score_athlete(CLUB, "João Baptista")
    print(json.dumps(profile, indent=2, default=str))

    print("\n── Upgrade candidates (score ≥ 60) ──────────")
    candidates = get_upgrade_candidates(CLUB, min_score=60, limit=5)
    for c in candidates:
        print(f"  {c['athlete']:25} score={c['upgrade_score']:3} "
              f"reasons={c['upgrade_reasons']}")

    print("\n✅ scorer.py working correctly")
