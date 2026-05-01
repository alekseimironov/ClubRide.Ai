"""
tests/test_briefing.py
Simulates the Friday briefing — prints to console, no WhatsApp send.
Run: python -m tests.test_briefing
"""

from brain.scorer    import get_upgrade_candidates, get_service_due, get_ghosts
from brain.retriever import get_club_summary, clear_cache

clear_cache()  # force fresh read after retriever fix

CLUB = 318940


def run():
    print("\n" + "=" * 55)
    print("  FRIDAY BRIEFING SIMULATION")
    print("=" * 55)

    # ── Club summary ───────────────────────────────
    s = get_club_summary(CLUB)
    print(f"\nW{s['week']}/{s['year']}  |  "
          f"{s['active_this_week']} active riders  |  "
          f"{s['total_km_this_week']:,.0f} km total")
    print(f"Top rider: {s['top_athlete']}  ({s['top_km']:.0f} km)")

    # ── Upgrade candidates ─────────────────────────
    print("\n--- UPGRADE CANDIDATES (score >= 60) ---------")
    candidates = get_upgrade_candidates(CLUB, min_score=60, limit=10)
    if candidates:
        for c in candidates:
            reasons = ", ".join(c["upgrade_reasons"]) or "—"
            print(f"  {c['athlete']:28}"
                  f"  score={c['upgrade_score']:3}/100"
                  f"  {c['bike_brand']} {c['bike_model']}"
                  f"  [{reasons}]")
    else:
        print("  None above threshold")

    # ── Service due ────────────────────────────────
    print("\n--- SERVICE DUE --------------------------------")
    service = get_service_due(CLUB, limit=10)
    if service:
        for a in service:
            km = float(a.get("Km_Since_Service", 0) or 0)
            print(f"  {a['Athlete']:28}"
                  f"  {km:6,.0f} km since service"
                  f"  ({a.get('Bike_Brand','')} {a.get('Bike_Model','')})"
                  f"  [estimated]")
    else:
        print("  No service alerts pending")

    # ── Ghost members ──────────────────────────────
    print("\n--- GHOST MEMBERS (absent 4+ weeks) -----------")
    ghosts = get_ghosts(CLUB)
    if ghosts:
        for g in ghosts[:10]:
            print(f"  {g['Full_Name']}  ({g.get('Membership','member')})")
    else:
        print("  No ghost members detected")

    print("\n" + "=" * 55)
    print(f"  Total signals: "
          f"{len(candidates)} upgrades  |  "
          f"{len(service)} services  |  "
          f"{len(ghosts)} ghosts")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    run()
