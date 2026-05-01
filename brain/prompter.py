"""
brain/prompter.py
Gemini function calling replaces keyword classifier entirely.

Flow:
  1. User message → Gemini Flash (picks tool + extracts entities)
  2. Tool executes → retriever returns real data
  3. Response: formatted text (5 tools) or second Gemini call (athlete profile)

No keywords. No regex. Any language, any phrasing.
"""

import os
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from google import genai
from google.genai import types

from .retriever import (
    get_leaderboard, get_club_summary,
    get_service_alerts, get_chain_alerts,
    get_athlete_profile, get_upgrade_candidates,
    get_at_risk_members, get_club_tier_summary,
    get_potential_recruits, get_week_attendees,
    get_weekend_priorities, clear_cache,
)
from .scorer import get_service_due, get_ghosts
from .session import get_history, add_turn
from .feedback import log_action, build_alert

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

_client  = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
CLUB_ID  = 318940

# ── Tool definitions ───────────────────────────────────────────────────────────

_TOOLS = types.Tool(function_declarations=[

    types.FunctionDeclaration(
        name="get_leaderboard",
        description=(
            "Show this week's TNCE leaderboard — top riders ranked by km ridden. "
            "Use for: 'top 10', 'classement', 'who rode the most', 'leaderboard'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "top_n": types.Schema(
                    type=types.Type.INTEGER,
                    description="Number of riders to show. Default 10."
                )
            }
        )
    ),

    types.FunctionDeclaration(
        name="get_athlete_profile",
        description=(
            "Get full profile for a specific athlete: weekly km, bike, tier, "
            "events attended, service status, upgrade signal. "
            "Use for: 'tell me about X', 'who is X', 'comment va X', 'X profile', "
            "'show me X', 'infos sur X', any message mentioning a person's name."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "athlete_name": types.Schema(
                    type=types.Type.STRING,
                    description="Full name or partial name of the athlete."
                )
            },
            required=["athlete_name"]
        )
    ),

    types.FunctionDeclaration(
        name="get_upgrade_candidates",
        description=(
            "List athletes who train seriously but ride a lower-tier bike — "
            "strongest upgrade targets for the shop. Shows weekly km, current bike, "
            "total mileage, and purchase source (TNCE or External). "
            "Use for: 'upgrade', 'who should buy a new bike', 'upgrade candidates', "
            "'qui doit changer de vélo', 'bike upgrade'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "limit": types.Schema(
                    type=types.Type.INTEGER,
                    description="Max number of candidates. Default 8."
                )
            }
        )
    ),

    types.FunctionDeclaration(
        name="get_service_alerts",
        description=(
            "List athletes whose bike is overdue for a service or chain replacement. "
            "Use for: 'service', 'entretien', 'who needs service', 'chain', "
            "'qui a besoin d entretien', 'maintenance'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "type": types.Schema(
                    type=types.Type.STRING,
                    description="'service' for full service alerts, 'chain' for chain-only. Default 'service'."
                )
            }
        )
    ),

    types.FunctionDeclaration(
        name="get_at_risk_members",
        description=(
            "List regular event attendees who have gone quiet recently — "
            "attended at least 5 events historically but absent for 6+ weeks. "
            "Shows attendance rate (events attended / total events since joining) "
            "and weeks since last appearance. "
            "Use for: 'at risk', 'who disappeared', 'inactive', 'ghost', 'missing members', "
            "'qui a disparu', 'inactifs', 'regulars gone quiet', 'retention'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={}
        )
    ),

    types.FunctionDeclaration(
        name="get_weekend_priorities",
        description=(
            "Returns the single top upgrade candidate and top service candidate "
            "for the owner to contact this weekend. Includes a suggested opening "
            "question for each in French. "
            "Use for: 'who should I talk to', 'qui contacter ce weekend', "
            "'priorities', 'who to call', 'act this weekend', 'what should I do'."
        ),
        parameters=types.Schema(type=types.Type.OBJECT, properties={})
    ),

    types.FunctionDeclaration(
        name="get_potential_recruits",
        description=(
            "Serious Strava followers who ride a lot but have NEVER attended a club event. "
            "Invitation targets — solo riders worth personally inviting to a group ride. "
            "Use for: 'potential members', 'recruit', 'solo riders', 'who should join us', "
            "'who rides alone', 'invite candidates', 'new members', 'qui devrait nous rejoindre'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "limit": types.Schema(type=types.Type.INTEGER,
                                      description="Max results. Default 10.")
            }
        )
    ),

    types.FunctionDeclaration(
        name="get_briefing",
        description=(
            "Full Friday club briefing combining: leaderboard summary, rider tier "
            "breakdown, upgrade candidates, service alerts, and at-risk members. "
            "Use for: 'briefing', 'résumé', 'summary', 'weekly report', "
            "'vendredi', 'rapport', 'what happened this week', 'full report'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={}
        )
    ),

    types.FunctionDeclaration(
        name="draft_message",
        description=(
            "Draft a ready-to-send WhatsApp message for a specific athlete, "
            "personalised based on their signal: service due, upgrade candidate, or engagement. "
            "Use for: 'draft for [name]', 'message for [name]', 'write to [name]', "
            "'what should I say to [name]', 'draft a message for [name]'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "athlete_name": types.Schema(
                    type=types.Type.STRING,
                    description="Full name or partial name of the athlete."
                )
            },
            required=["athlete_name"]
        )
    ),

    types.FunctionDeclaration(
        name="show_help",
        description=(
            "Show the command menu. Use this when the message is off-topic, "
            "unrelated to the cycling club, cannot be answered by any other tool, "
            "or is a general question (weather, jokes, news, chitchat, etc.). "
            "Also use for: 'full list of members', 'all members', 'show all', "
            "'list members', 'how many members', 'complete list', or any request "
            "for a raw member list — there is no tool for that."
        ),
        parameters=types.Schema(type=types.Type.OBJECT, properties={})
    ),

])

_ROUTING_SYSTEM = """You are ClubRide.Ai, assistant for TNCE cycling club owner Aleksei in Lausanne.

Always call exactly one tool. Never answer directly.

Tool selection rules (apply in order, stop at first match):
1. Asks to draft/write/message to a person by name → draft_message
2. Message mentions a person's name → get_athlete_profile
3. Asks about this week's ranking or who rode the most → get_leaderboard
4. Asks about bike upgrades or who should buy a new bike → get_upgrade_candidates
5. Asks about service, chain, or maintenance → get_service_alerts
6. Asks about inactive, missing, or ghost members → get_at_risk_members
7. Asks who to contact or priorities for this weekend → get_weekend_priorities
8. Asks about recruiting solo riders → get_potential_recruits
9. Explicitly says "briefing", "weekly report", "résumé", or "full report" → get_briefing
10. Anything else — off-topic, unclear, general question, chitchat, member list requests → show_help

get_briefing is NOT a member directory. Never use it to answer "list of members" or "full list" requests.
When in doubt, call show_help."""

_ATHLETE_SYSTEM = """You are ClubRide.Ai, smart assistant for TNCE cycling club owner Aleksei.
You are writing TO Aleksei ABOUT the athlete — never address the athlete directly.
Write a WhatsApp message — maximum 3 sentences, plain text, no markdown headers.
Refer to the athlete by first name in third person (e.g. "Marko rides...", "He is...").
Highlight the most important insight: upgrade opportunity, service need, or loyalty.
If data_quality is 'attendance_only', mention that full profile is not yet available.
Never invent numbers — use only the data provided."""

_DRAFT_SYSTEM = """You are ClubRide.Ai helping a cycling club owner draft a short WhatsApp message to send directly to an athlete.

Rules:
- English only
- 2-3 sentences maximum, conversational tone
- Warm and personal, not salesy or pushy
- Start with "Hey [FirstName],"
- Never mention the owner's name or ClubRide.Ai
- Always end with a clear call to action:
  * upgrade → invite them to come see new bikes in the shop
  * service → ask them to book the bike in
  * ghost → low-pressure personal invite: coffee at the shop, no obligation
  * engagement → open-ended invitation to stop by
- If the owner's note contains extra instructions (specific bike model, event, discount, etc.), honour them and weave them naturally into the message"""


# ── Formatters (deterministic, zero Gemini) ────────────────────────────────────

def _fmt_leaderboard(club_id: int, top_n: int = 10) -> str:
    from .retriever import _norm, _load_csv, _paths
    import pandas as pd

    # Load full leaderboard (get more than top_n to filter)
    data     = get_leaderboard(club_id, top_n=200)
    athletes = data.get("athletes", [])
    week, year = data.get("week"), data.get("year")
    if not athletes:
        return "No leaderboard data available yet."

    # Build community member set from all attendance history
    p      = _paths(club_id)
    enr_df = _load_csv(p["enriched"])
    community_norms = set()
    if not enr_df.empty:
        for name in enr_df["Athlete_Raw"].dropna():
            community_norms.add(_norm(str(name)))
        for name in enr_df["Matched_Name"].dropna():
            community_norms.add(_norm(str(name)))

    # Filter to community members only
    community = [a for a in athletes if _norm(a["Athlete"]) in community_norms]
    shown     = community[:top_n]

    # This week's event attendance — past and future
    att        = get_week_attendees(club_id, week, year)
    past       = att.get("past", set())
    future     = att.get("future", set())
    evt_dates  = att.get("event_dates", [])

    past_count   = sum(1 for a in shown if _norm(a["Athlete"]) in past)
    future_count = sum(1 for a in shown if _norm(a["Athlete"]) in future
                       and _norm(a["Athlete"]) not in past)

    lines = [f"🏆 *W{week}/{year} Community Top {len(shown)}*"]
    subtitle = []
    if past_count:
        subtitle.append(f"{past_count} came to the club ride")
    if future_count:
        # Find upcoming event date for context
        upcoming = next((f"{d.strftime('%A %b %d')}" for d, t, f in evt_dates if f), "this week")
        subtitle.append(f"{future_count} registered for {upcoming}")
    if subtitle:
        lines.append(f"_{'  ·  '.join(subtitle)}_\n")
    else:
        lines.append("")

    for i, a in enumerate(shown, 1):
        n = _norm(a["Athlete"])
        if n in past:
            icon = "✅"
        elif n in future:
            icon = "📅"
        else:
            icon = "  "
        lines.append(
            f"{icon} {i}. {a['Athlete']} — {a['Distance_km']:.0f}km"
            f" ({int(a['Rides'] or 0)} rides)"
        )

    if not shown:
        return f"No community members on the W{week}/{year} leaderboard yet."

    return "\n".join(lines)


def _fmt_service(club_id: int, alert_type: str = "service") -> str:
    if alert_type == "chain":
        data     = get_chain_alerts(club_id, limit=10)
        athletes = data.get("athletes", [])
        if not athletes:
            return "No chain replacement alerts."
        lines = [f"Chain Due ({len(athletes)} athletes)\n"]
        for a in athletes:
            km   = float(a.get("Km_Since_Chain", 0) or 0)
            bike = a.get("Bike_Model") or a.get("Bike_Brand") or "unknown"
            lines.append(f"• {a['Athlete']} — {km:,.0f}km since chain · {bike}")
    else:
        alerts = get_service_due(club_id, limit=10)
        if not alerts:
            return "No service alerts pending."
        lines = [f"Service Due ({len(alerts)} athletes) *[estimated data]*\n"]
        for a in alerts:
            km   = float(a.get("Km_Since_Service", 0) or 0)
            bike = a.get("Bike_Model") or a.get("Bike_Brand") or "unknown"
            last = str(a.get("Last_Service_Date", "") or "")
            last_str = f" · last: {last[:7]}" if last and last != "nan" else ""
            lines.append(f"• {a['Athlete']} — {km:,.0f}km since service{last_str} · {bike}")

    return "\n".join(lines)


def _fmt_upgrade(club_id: int, limit: int = 8) -> str:
    data       = get_upgrade_candidates(club_id, limit=limit)
    candidates = data.get("candidates", [])
    if not candidates:
        return "No upgrade candidates right now."
    # Sort: loyal regulars first, then by weekly km
    candidates = sorted(candidates,
                        key=lambda c: (-int(c.get("events_count") or 0),
                                       -float(c.get("weekly_km") or 0)))
    lines = [f"Upgrade Candidates ({len(candidates)})\n"]
    for c in candidates:
        # Weekly km
        wk     = float(c.get("weekly_km") or 0)
        km_src = c.get("km_source", "real")
        wk_str = f"~{wk:.0f} km/wk (est.)" if km_src == "estimated" else f"{wk:.0f} km/wk"

        # Bike
        bike      = c.get("primary_bike") or ""
        bk_src    = c.get("bike_source", "unknown")
        has_bike  = bool(bike and bike not in ("nan", "unknown", ""))
        if not has_bike:
            bike_str = "bike unknown"
        elif bk_src == "inferred":
            bike_str = f"{bike} (inferred)"
        else:
            bike_str = bike

        # Bike tier
        pt = c.get("primary_tier") or ""
        tier_str = f"({pt} tier)" if pt and pt not in ("nan", "unknown", "") else ""

        # Fleet km — only for real bike data
        bike_km  = float(c.get("primary_bike_km") or 0)
        km_str   = f" · {bike_km:,.0f}km on this bike" if bk_src == "real" and bike_km > 0 else ""

        # Speed estimate for no-profile athletes
        spd = float(c.get("speed_est") or 0)
        spd_str = f" · {spd:.1f}km/h avg" if spd > 0 and not c.get("has_profile") else ""

        # Purchase
        src     = c.get("purchase_source", "")
        yr      = c.get("purchase_year", "")
        src_str = f" · bought TNCE {yr} (est.)" if src == "Club TNCE" and yr else \
                  f" · bought External (est.)" if src else ""

        # Loyalty
        ev = int(c.get("events_count") or 0)
        if ev == 0:    loyalty = "solo rider"
        elif ev < 4:   loyalty = f"occasional ({ev} events)"
        elif ev < 10:  loyalty = f"regular ({ev} events)"
        else:          loyalty = f"loyal ({ev} events)"

        lines.append(
            f"• {c['name']} — {wk_str}"
            f" · {bike_str} {tier_str}{km_str}{spd_str}"
            f" · {loyalty}{src_str}"
        )
    return "\n".join(lines)


def _fmt_weekend_priorities(club_id: int) -> str:
    data    = get_weekend_priorities(club_id)
    upgrade = data.get("upgrade")
    service = data.get("service")

    if not upgrade and not service:
        return "No priorities this weekend — all clear."

    lines = ["*ACT THIS WEEKEND*\n"]

    if upgrade:
        bike     = upgrade.get("primary_bike") or ""
        bike     = "bike" if not bike or bike in ("nan", "unknown") else bike
        fleet_km = float(upgrade.get("fleet_km") or 0)
        km_str   = f" · {fleet_km:,.0f}km" if fleet_km > 0 else ""
        ev       = int(upgrade.get("events_count") or 0)
        src      = upgrade.get("purchase_source", "")
        yr       = upgrade.get("purchase_year", "")
        src_str  = f" · bought {src} {yr}" if src and yr else ""
        wk       = float(upgrade.get("weekly_km") or 0)
        draft    = _draft_whatsapp(upgrade["name"], "upgrade",
                                   weekly_km=wk, bike=bike, events=ev)
        lines.append(
            f"⭐ *{upgrade['name']}*\n"
            f"   Upgrade window · {wk:.0f}km/wk"
            f" · {bike}{km_str}{src_str} · {ev} events\n"
            f"   📱 _{draft}_"
        )

    if service:
        km_since = float(service.get("km_since") or 0)
        draft    = _draft_whatsapp(service["name"], "service", km_since=km_since)
        lines.append(
            f"\n🔧 *{service['name']}*\n"
            f"   Service overdue · {km_since:,.0f}km since last\n"
            f"   📱 _{draft}_"
        )

    return "\n".join(lines)


def _fmt_recruits(club_id: int, limit: int = 10) -> str:
    data     = get_potential_recruits(club_id, limit=limit)
    recruits = data.get("recruits", [])
    if not recruits:
        return "No solo riders to recruit right now."
    lines = [f"Potential recruits ({len(recruits)}) — serious riders who never joined a ride\n"]
    for r in recruits:
        bike = r.get("primary_bike") or ""
        bike = "no bike data" if not bike or bike == "nan" else bike
        loc  = r.get("location", "")
        loc  = "" if not loc or loc == "nan" else loc
        lines.append(
            f"• {r['name']} — {r['weekly_km']:.0f} km/wk"
            f" · {bike} ({r['primary_tier']} tier)"
            f" · {r['rider_tier']} rider"
            + (f" · {loc}" if loc else "")
        )
    lines.append("\nThese riders are active but ride solo — worth a personal invitation.")
    return "\n".join(lines)


def _fmt_at_risk(club_id: int) -> str:
    data    = get_at_risk_members(club_id)
    members = data.get("at_risk", [])
    if not members:
        return "No at-risk regulars detected — everyone is showing up."
    lines = [f"At-risk regulars ({len(members)})\n"]
    for m in members:
        lines.append(
            f"• {m['name']} — {m['attended']}/{m['total_events']} events"
            f" ({m['rate_pct']}%) · last seen {m['weeks_absent']}w ago"
        )
    return "\n".join(lines)


def _draft_whatsapp(name: str, signal: str, hint: str = "", **kwargs) -> str:
    first = name.split()[0]
    if signal == "service":
        km   = float(kwargs.get("km_since", 0) or 0)
        bike = kwargs.get("bike", "your bike")
        ctx  = (f"Athlete first name: {first}\n"
                f"Bike: {bike}\n"
                f"Km since last service: {km:,.0f}km\n"
                f"Reason: bike is overdue for service")
    elif signal == "upgrade":
        wk   = float(kwargs.get("weekly_km", 0) or 0)
        bike = kwargs.get("bike", "their current bike")
        ev   = int(kwargs.get("events", 0) or 0)
        ctx  = (f"Athlete first name: {first}\n"
                f"Current bike: {bike}\n"
                f"Weekly km: {wk:.0f}km\n"
                f"Club events attended: {ev}\n"
                f"Reason: strong rider on a lower-tier bike, good upgrade candidate")
    elif signal == "ghost":
        weeks = int(kwargs.get("weeks", 0) or 0)
        ev    = int(kwargs.get("events", 0) or 0)
        ctx   = (f"Athlete first name: {first}\n"
                 f"Club events attended historically: {ev}\n"
                 f"Weeks since last seen at club ride: {weeks}\n"
                 f"Reason: was a regular member but has stopped showing up")
    else:
        wk  = float(kwargs.get("weekly_km", 0) or 0)
        ev  = int(kwargs.get("events", 0) or 0)
        ctx = (f"Athlete first name: {first}\n"
               f"Weekly km: {wk:.0f}km\n"
               f"Club events attended: {ev}\n"
               f"Reason: general re-engagement")
    if hint:
        ctx += f"\nOwner's note: {hint}"
    try:
        resp = _client.models.generate_content(
            model="gemini-2.5-flash",
            contents=ctx,
            config=types.GenerateContentConfig(
                system_instruction=_DRAFT_SYSTEM,
                temperature=0.7,
            )
        )
        return resp.text.strip()
    except Exception:
        if signal == "service":
            return f"Hey {first}, your bike is coming up on {float(kwargs.get('km_since', 0)):,.0f}km — good time to bring it in for a service before summer."
        if signal == "upgrade":
            return f"Hey {first}, you've been putting in serious km lately — we have some great new bikes in the shop worth a look."
        if signal == "ghost":
            return f"Hey {first}, haven't seen you at the club rides in a while — hope everything's good, would love to have you back."
        return f"Hey {first}, haven't seen you around lately — would love to catch up at the shop."


def _handle_draft_message(club_id: int, athlete_name: str, question: str = "") -> str:
    p = get_athlete_profile(club_id, athlete_name)
    if not p.get("found"):
        return f"No data found for '{athlete_name}' — not in the event attendance records."

    name    = p.get("name", athlete_name)
    svc_due = p.get("service_due", False)
    chn_due = p.get("chain_due", False)
    svc_km  = float(p.get("km_since_service", 0) or 0)
    chn_km  = float(p.get("km_since_chain", 0) or 0)
    rtier   = p.get("rider_tier", "")
    btier   = p.get("primary_tier", "")
    bike    = p.get("primary_bike") or "bike"
    bike    = "bike" if bike in ("nan", "unknown", "") else bike
    wk      = float(p.get("weekly_km", 0) or 0)
    ev      = p.get("events_count", 0)

    # At-risk: calculate weeks since last seen
    weeks_absent = 0
    last_seen = p.get("last_seen", "")
    if last_seen:
        try:
            from datetime import datetime as _dt
            last_dt      = _dt.strptime(str(last_seen)[:10], "%Y-%m-%d").date()
            weeks_absent = (date.today() - last_dt).days // 7
        except Exception:
            pass
    is_at_risk = weeks_absent >= 6 and ev >= 5

    # Priority: upgrade (highest commercial value) → service → at-risk → engagement
    if rtier in ("top", "mid") and btier in ("mid", "entry"):
        draft  = _draft_whatsapp(name, "upgrade", hint=question, weekly_km=wk, bike=bike, events=ev)
        reason = f"upgrade candidate · {wk:.0f}km/wk on {bike}"
    elif svc_due:
        draft  = _draft_whatsapp(name, "service", hint=question, km_since=svc_km, bike=bike)
        reason = f"service due · {svc_km:,.0f}km since last"
    elif chn_due:
        draft  = _draft_whatsapp(name, "service", hint=question, km_since=chn_km, bike=bike)
        reason = f"chain due · {chn_km:,.0f}km since last"
    elif is_at_risk:
        draft  = _draft_whatsapp(name, "ghost", hint=question, weeks=weeks_absent, events=ev)
        reason = f"inactive · {weeks_absent}w since last seen"
    else:
        draft  = _draft_whatsapp(name, "engagement", hint=question, weekly_km=wk, events=ev)
        reason = "general engagement"

    return (f"*Draft for {name.split()[0]}* ({reason})\n\n"
            f"{draft}\n\n"
            f"_(copy & send on WhatsApp)_")


def _fmt_briefing(club_id: int) -> str:
    from .retriever import _norm, _load_csv, _paths, _load_leaderboard
    import pandas as _pd

    sections = []
    p      = _paths(club_id)
    enr_df = _load_csv(p["enriched"])
    prof_df= _load_csv(p["profiles"])

    # Community member norms
    community = set()
    if not enr_df.empty:
        for name in enr_df["Athlete_Raw"].dropna():
            community.add(_norm(str(name)))

    # Community leaderboard stats this week
    lb_df = _load_leaderboard(club_id)
    week = year = active = total_km = top_athlete = top_km = None
    if not lb_df.empty:
        latest_year = lb_df["Year"].max()
        latest_week = lb_df[lb_df["Year"] == latest_year]["Week_Number"].max()
        week, year  = int(latest_week), int(latest_year)
        w_df        = lb_df[(lb_df["Week_Number"] == latest_week) &
                            (lb_df["Year"] == latest_year)]
        best        = (w_df.sort_values("Snapshot_Date", ascending=False)
                          .groupby("Athlete")["Distance_km"].max())
        comm_best   = best[best.index.map(_norm).isin(community)]
        active      = len(comm_best)
        total_km    = round(comm_best.sum(), 0)
        if not comm_best.empty:
            top_athlete = comm_best.idxmax()
            top_km      = round(comm_best.max(), 0)

    sections.append(
        f"TNCE Briefing · W{week}/{year}\n"
        f"Community active: {active} riders · {total_km:,.0f}km\n"
        f"Top: {top_athlete} ({top_km:.0f}km)" if top_athlete else
        f"TNCE Briefing · W{week}/{year}\nNo community leaderboard data yet."
    )

    # Community-only tier split + bike brands
    if not prof_df.empty:
        prof_df["_norm"] = prof_df["Name"].apply(_norm)
        comm_prof = prof_df[prof_df["_norm"].isin(community)]
        td        = comm_prof["rider_tier"].value_counts()
        sections.append(
            f"\nRiders: top {td.get('top',0)} · mid {td.get('mid',0)} · entry {td.get('entry',0)}"
            f" (of {len(community)} community members)"
        )
        # Bike brands
        bikes_df = _load_csv(p["profiles"].parent / "../real/athlete_bikes.csv"
                             if False else p["profiles"].parent / "athlete_bikes.csv")
        # fallback path
        from pathlib import Path as _Path
        bikes_path = _Path(p["profiles"]).parent / "athlete_bikes.csv"
        try:
            bk = _pd.read_csv(bikes_path, dtype=str)
            comm_ids = comm_prof["Athlete_ID"].astype(str).tolist()
            comm_bk  = bk[bk["Athlete_ID"].astype(str).isin(comm_ids)]
            top_brands = (comm_bk["Brand"].value_counts()
                         .head(6)
                         .index.tolist())
            if top_brands:
                brand_str = " · ".join(
                    f"{b}({comm_bk['Brand'].value_counts()[b]})"
                    for b in top_brands
                )
                sections.append(f"\nCommunity bikes: {brand_str}")
        except Exception:
            pass

    # Upgrade candidates
    upg  = get_upgrade_candidates(club_id, limit=5)
    cands = upg.get("candidates", [])
    if cands:
        lines = [f"\nUpgrade ({len(cands)})"]
        for c in cands:
            bike     = c.get("primary_bike") or ""
            has_bike = bool(bike and bike not in ("nan", "unknown", ""))
            bike     = bike if has_bike else "no bike data"
            fleet_km = float(c.get("fleet_km") or 0)
            km_str   = f" · {fleet_km:,.0f}km" if has_bike and fleet_km > 0 else ""
            lines.append(f"• {c['name']} — {c['weekly_km']:.0f}km/wk · {bike}{km_str}")
        sections.append("\n".join(lines))
    else:
        sections.append("\nUpgrade — none flagged")

    # Service alerts
    alerts = get_service_due(club_id, limit=5)
    if alerts:
        lines = [f"\nService Due ({len(alerts)})"]
        for a in alerts:
            km   = float(a.get("Km_Since_Service", 0) or 0)
            bike = a.get("Bike_Model") or a.get("Bike_Brand") or "?"
            lines.append(f"• {a['Athlete']} — {km:,.0f}km · {bike}")
        sections.append("\n".join(lines))
    else:
        sections.append("\nService — all clear")

    # At-risk
    risk = get_at_risk_members(club_id)
    members = risk.get("at_risk", [])
    if members:
        lines = [f"\nAt-risk ({len(members)})"]
        for m in members:
            lines.append(
                f"• {m['name']} — {m['attended']}/{m['total_events']}"
                f" ({m['rate_pct']}%) · {m['weeks_absent']}w ago"
            )
        sections.append("\n".join(lines))
    else:
        sections.append("\nAt-risk — all regulars active")

    return "\n".join(sections)


# ── Athlete profile — deterministic, facts only ───────────────────────────────

def _handle_athlete(club_id: int, athlete_name: str,
                    question: str, history: list[dict]) -> str:
    p = get_athlete_profile(club_id, athlete_name)

    if not p.get("found"):
        return f"No data found for {athlete_name} — not in the event attendance records."

    name   = p.get("name", athlete_name)
    ev     = p.get("events_count", 0)
    first  = p.get("first_seen", "")
    last   = p.get("last_seen", "")

    # Attendance-only — no Strava profile available
    if p.get("data_quality") == "attendance_only":
        lines = [f"*{name}*"]
        lines.append(f"Events: {ev}  ·  First: {first}  ·  Last: {last}")
        lines.append("Full profile coming soon.")
        return "\n".join(lines)

    # Full profile — deterministic facts
    wk       = float(p.get("weekly_km", 0) or 0)
    alltime  = float(p.get("alltime_km", 0) or 0)
    speed    = float(p.get("avg_speed", 0) or 0)
    longest  = float(p.get("longest_ride", 0) or 0)
    rtier    = p.get("rider_tier", "unknown")
    bike     = p.get("primary_bike") or ""
    bike     = "" if bike in ("nan", "unknown") else bike
    btier    = p.get("primary_tier", "")
    btier    = "" if btier in ("nan", "unknown") else btier
    bike_km  = float(p.get("primary_bike_km", 0) or 0)
    fleet_km = float(p.get("fleet_km", 0) or 0)
    svc_due  = p.get("service_due", False)
    chn_due  = p.get("chain_due", False)
    svc_km   = float(p.get("km_since_service", 0) or 0)
    chn_km   = float(p.get("km_since_chain", 0) or 0)
    src      = p.get("purchase_source", "")
    loc      = p.get("location", "")

    # Purchase duration
    pd_str = p.get("purchase_date", "")
    months_owned = ""
    if pd_str and len(pd_str) >= 7:
        try:
            from datetime import datetime as _dt
            pd_date      = _dt.strptime(pd_str[:7], "%Y-%m").date()
            months_owned = (date.today().year - pd_date.year) * 12 + \
                           (date.today().month - pd_date.month)
        except Exception:
            pass

    lines = [f"*{name}*"]
    if loc and loc not in ("nan", ""):
        lines[0] += f"  ·  {loc}"

    # Riding stats
    stat_parts = []
    if wk > 0:    stat_parts.append(f"{wk:.0f} km/wk")
    if alltime > 0: stat_parts.append(f"{alltime:,.0f}km lifetime")
    if speed > 0: stat_parts.append(f"{speed:.1f} km/h avg")
    if longest > 0: stat_parts.append(f"longest {longest:.0f}km")
    if stat_parts:
        lines.append("  ·  ".join(stat_parts))

    # Purchase date as Month Year
    purchase_str = ""
    if pd_str and len(pd_str) >= 7:
        try:
            from datetime import datetime as _dt2
            pd_date2     = _dt2.strptime(pd_str[:7], "%Y-%m")
            purchase_str = pd_date2.strftime("%b %Y")
        except Exception:
            pass

    # Bike line
    bike_parts = []
    if bike:     bike_parts.append(bike)
    if btier:    bike_parts.append(f"{btier} tier")
    if bike_km > 0: bike_parts.append(f"{bike_km:,.0f}km on bike")
    if src and purchase_str:
        bike_parts.append(f"bought {src} {purchase_str} (est.)")
    elif src:
        bike_parts.append(f"bought {src} (est.)")
    if bike_parts:
        lines.append("  ·  ".join(bike_parts))

    # Events
    ev_line = f"{ev} events"
    if first: ev_line += f"  ·  first {first}"
    if last:
        try:
            last_date  = date.fromisoformat(str(last)[:10])
            last_label = "upcoming" if last_date > date.today() else "last"
        except Exception:
            last_label = "last"
        ev_line += f"  ·  {last_label} {last}"
    lines.append(ev_line)

    # Alerts
    alerts = []
    if svc_due:  alerts.append(f"service due ({svc_km:,.0f}km since last)")
    if chn_due:  alerts.append(f"chain due ({chn_km:,.0f}km since last)")
    if alerts:
        lines.append("⚠️ " + "  ·  ".join(alerts))

    # Upgrade flag — only if clear mismatch
    if rtier == "top" and btier in ("mid", "entry"):
        lines.append("Upgrade opportunity — serious rider, bike below their level.")
    elif rtier == "mid" and btier == "entry":
        lines.append("Potential upgrade — regular rider on entry bike.")

    return "\n".join(lines)


# ── Tool executor ──────────────────────────────────────────────────────────────

def _execute_tool(tool_name: str, args: dict,
                  club_id: int, question: str,
                  history: list[dict]) -> str:
    if tool_name == "get_leaderboard":
        return _fmt_leaderboard(club_id, top_n=int(args.get("top_n", 10)))

    if tool_name == "get_athlete_profile":
        return _handle_athlete(club_id, args.get("athlete_name", ""), question, history)

    if tool_name == "draft_message":
        return _handle_draft_message(club_id, args.get("athlete_name", ""), question)

    if tool_name == "get_upgrade_candidates":
        return _fmt_upgrade(club_id, limit=int(args.get("limit", 8)))

    if tool_name == "get_service_alerts":
        return _fmt_service(club_id, alert_type=args.get("type", "service"))

    if tool_name == "get_at_risk_members":
        return _fmt_at_risk(club_id)

    if tool_name == "get_weekend_priorities":
        return _fmt_weekend_priorities(club_id)

    if tool_name == "get_potential_recruits":
        return _fmt_recruits(club_id, limit=int(args.get("limit", 10)))

    if tool_name == "get_briefing":
        return _fmt_briefing(club_id)

    if tool_name == "show_help":
        return HELP_TEXT

    return HELP_TEXT


# ── Main entry point ───────────────────────────────────────────────────────────

HELP_TEXT = """🚴 *ClubRide.Ai* — TNCE club assistant

*Commands:*
🏆 *top 10* — this week's leaderboard
🎯 *who to talk to* — 2 priorities for this weekend
⭐ *upgrade* — community members who should buy a new bike
🔧 *service* — bikes due for service or chain
⚠️ *at risk* — regulars who stopped showing up
👥 *recruit* — serious solo riders to invite to the group
📋 *briefing* — full weekly report

*Ask about any rider:*
👤 "tell me about Marko"
👤 "who is John Custo"
👤 "Julien Loisy"

"""


def handle(message: str, owner_id: str,
           club_id: int = CLUB_ID) -> str:
    history = get_history(owner_id)

    import re

    # Help
    if message.strip().lower() in ("help", "aide", "?", "commands", "menu"):
        add_turn(owner_id, message, HELP_TEXT)
        return HELP_TEXT

    # Feedback reply (1-4 digit)
    if re.match(r"^\s*[1-4]\s*$", message.strip()):
        action_map = {"1": "done", "2": "contacted", "3": "snoozed", "4": "ignored"}
        action = action_map.get(message.strip())
        athlete = None
        for msg in reversed(history[-4:]):
            if msg["role"] == "assistant":
                m = re.search(r"•\s+([A-Z][^\n—·]+?)(?:\s+—|\s+·)", msg["content"])
                if m:
                    athlete = m.group(1).strip()
                    break
        if action and athlete:
            log_action(owner_id, athlete, "service_due", action)
            reply = f"Got it — logged '{action}' for {athlete}."
        else:
            reply = "Reply 1-4 after an alert, or ask me anything about the club."
        add_turn(owner_id, message, reply)
        return reply

    # Build conversation context — include history for memory
    if history:
        session_turns = []
        for h in history[-4:]:
            role = "user" if h["role"] == "user" else "model"
            session_turns.append(types.Content(role=role,
                                               parts=[types.Part(text=h["content"])]))
        session_turns.append(types.Content(role="user",
                                           parts=[types.Part(text=message)]))
        contents = session_turns
    else:
        contents = message   # first message — plain string works best

    # ── Keyword fallback — works even when Gemini is 503 ──────────────────────
    # Keep entries specific — avoid broad words that can appear in off-topic messages
    msg_low = message.strip().lower()
    _KW = {
        "top 10":        ("get_leaderboard", {}),
        "top10":         ("get_leaderboard", {}),
        "leaderboard":   ("get_leaderboard", {}),
        "classement":    ("get_leaderboard", {}),
        "upgrade":       ("get_upgrade_candidates", {}),
        "service":       ("get_service_alerts", {"type": "service"}),
        "chain":         ("get_service_alerts", {"type": "chain"}),
        "entretien":     ("get_service_alerts", {"type": "service"}),
        "at risk":       ("get_at_risk_members", {}),
        "at-risk":       ("get_at_risk_members", {}),
        "ghost":         ("get_at_risk_members", {}),
        "recruit":       ("get_potential_recruits", {}),
        "draft for":     ("draft_message", {}),
        "message for":   ("draft_message", {}),
        "write to":      ("draft_message", {}),
        "briefing":      ("get_briefing", {}),
        "résumé":        ("get_briefing", {}),
        "weekly report": ("get_briefing", {}),
        "who to talk":   ("get_weekend_priorities", {}),
        "qui contacter": ("get_weekend_priorities", {}),
        "priorities":    ("get_weekend_priorities", {}),
        "priorités":     ("get_weekend_priorities", {}),
    }
    for kw, (tool, args) in _KW.items():
        if kw in msg_low:
            if tool == "draft_message":
                # Extract name after the keyword
                idx  = msg_low.index(kw) + len(kw)
                name = message[idx:].strip().strip("?.,!")
                if not name:
                    continue
                args = {"athlete_name": name}
            try:
                reply = _execute_tool(tool, args, club_id, message, history)
            except Exception as e:
                print(f"  Keyword fallback error [{tool}]: {e}")
                reply = "Something went wrong. Please try again."
            add_turn(owner_id, message, reply)
            return reply

    # Gemini picks the tool — 3 retries with 2s backoff on 503
    import time as _time
    response = None
    for attempt in range(3):
        try:
            response = _client.models.generate_content(
                model="gemini-2.5-flash",
                contents=contents,
                config=types.GenerateContentConfig(
                    tools=[_TOOLS],
                    system_instruction=_ROUTING_SYSTEM,
                    temperature=0.1,
                )
            )
            break
        except Exception as e:
            err_str = str(e)
            print(f"  Gemini routing error (attempt {attempt+1}/3): {err_str[:120]}")
            if "503" in err_str and attempt < 2:
                _time.sleep(2 * (attempt + 1))
                continue
            reply = "Service temporarily busy — please try again in a moment."
            add_turn(owner_id, message, reply)
            return reply

    if response is None:
        reply = "Service temporarily busy — please try again in a moment."
        add_turn(owner_id, message, reply)
        return reply

    # Extract function call — ignore any text parts that look like raw tool calls
    for part in response.candidates[0].content.parts:
        if hasattr(part, "function_call") and part.function_call:
            fn    = part.function_call
            try:
                reply = _execute_tool(fn.name, dict(fn.args),
                                      club_id, message, history)
            except Exception:
                reply = "Something went wrong fetching that data. Please try again."
            add_turn(owner_id, message, reply)
            return reply

    # No tool call — off-topic question; show the command menu
    reply = HELP_TEXT

    add_turn(owner_id, message, reply)
    return reply
