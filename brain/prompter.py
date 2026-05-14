"""
brain/prompter.py
Gemini function calling replaces keyword classifier entirely.

Flow:
  1. User message → Gemini Flash (picks tool + extracts entities)
  2. Tool executes → retriever returns real data
  3. Response: formatted text (deterministic) or LLM call (draft messages)

No keywords. No regex. Any language, any phrasing.
"""

import os
import json
import requests
from datetime import date, timedelta
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
    get_weekend_priorities, get_missed_upgrades, clear_cache,
)
from .scorer import get_service_due
from .session import get_history, add_turn, get_lang, set_lang, get_last_athlete, set_last_athlete
from .feedback import log_action, build_alert
from .strings import t
from privacy_gate.masker import build_anonymiser

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

_client  = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
CLUB_ID  = 318940

# ── Tool definitions ───────────────────────────────────────────────────────────

_LANG_PARAM = types.Schema(
    type=types.Type.STRING,
    description='Detected language of the user\'s message: "en" or "fr".'
)

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
                "top_n": types.Schema(type=types.Type.INTEGER, description="Number of riders to show. Default 10."),
                "language": _LANG_PARAM,
            },
            required=["language"]
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
                "athlete_name": types.Schema(type=types.Type.STRING, description="Full name or partial name of the athlete."),
                "language": _LANG_PARAM,
            },
            required=["athlete_name", "language"]
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
                "limit": types.Schema(type=types.Type.INTEGER, description="Max number of candidates. Default 8."),
                "language": _LANG_PARAM,
            },
            required=["language"]
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
                "type": types.Schema(type=types.Type.STRING, description="'service' or 'chain'. Default 'service'."),
                "language": _LANG_PARAM,
            },
            required=["language"]
        )
    ),

    types.FunctionDeclaration(
        name="get_at_risk_members",
        description=(
            "List regular event attendees who have gone quiet recently — "
            "attended at least 5 events historically but absent for 6+ weeks. "
            "Use for: 'at risk', 'who disappeared', 'inactive', 'ghost', 'missing members', "
            "'qui a disparu', 'inactifs', 'regulars gone quiet', 'retention'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={"language": _LANG_PARAM},
            required=["language"]
        )
    ),

    types.FunctionDeclaration(
        name="get_weekend_priorities",
        description=(
            "Returns the top upgrade candidate and top service candidate for this weekend. "
            "Use for: 'who should I talk to', 'qui contacter ce weekend', "
            "'priorities', 'who to call', 'act this weekend', 'what should I do'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={"language": _LANG_PARAM},
            required=["language"]
        )
    ),

    types.FunctionDeclaration(
        name="get_potential_recruits",
        description=(
            "Serious Strava followers who ride a lot but have NEVER attended a club event. "
            "Use for: 'potential members', 'recruit', 'solo riders', 'who should join us', "
            "'who rides alone', 'invite candidates', 'new members', 'qui devrait nous rejoindre'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "limit": types.Schema(type=types.Type.INTEGER, description="Max results. Default 10."),
                "language": _LANG_PARAM,
            },
            required=["language"]
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
            properties={"language": _LANG_PARAM},
            required=["language"]
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
                "athlete_name": types.Schema(type=types.Type.STRING, description="Full name or partial name of the athlete."),
                "language": _LANG_PARAM,
            },
            required=["athlete_name", "language"]
        )
    ),

    types.FunctionDeclaration(
        name="get_loyal_members",
        description=(
            "Top community members by events attended — still actively showing up. "
            "Use for: 'loyal', 'top members', 'best attendees', 'community leaders', "
            "'most active members', 'who comes most'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={
                "limit": types.Schema(type=types.Type.INTEGER, description="Max results. Default 10."),
                "language": _LANG_PARAM,
            },
            required=["language"]
        )
    ),

    types.FunctionDeclaration(
        name="get_missed_upgrades",
        description=(
            "Show athletes who recently upgraded to a higher-tier bike — not through the shop. "
            "Use for: 'missed opportunity', 'missed upgrades', 'who upgraded elsewhere', "
            "'external upgrades', 'opportunités manquées', 'qui a upgradé sans nous'."
        ),
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={"language": _LANG_PARAM},
            required=["language"]
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
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={"language": _LANG_PARAM},
            required=["language"]
        )
    ),

])

_ROUTING_SYSTEM = """You are ClubRide.Ai, assistant for TNCE cycling club owner Aleksei in Lausanne.

Always call exactly one tool. Never answer directly.
Always include the `language` argument: "fr" if the message is in French, "en" otherwise.

Tool selection rules (apply in order, stop at first match):
1. Asks to draft/write/message to a person by name → draft_message
2. Message mentions a person's name → get_athlete_profile
3. Asks about this week's ranking or who rode the most → get_leaderboard
4. Asks about bike upgrades or who should buy a new bike → get_upgrade_candidates
5. Asks about service, chain, or maintenance → get_service_alerts
6. Asks about inactive, missing, or ghost members → get_at_risk_members
7. Asks who to contact or priorities for this weekend → get_weekend_priorities
8. Asks about recruiting solo riders → get_potential_recruits
9. Asks about loyal members, top attendees, most active, community leaders → get_loyal_members
10. Asks about missed upgrades, who upgraded elsewhere, external bike purchases, opportunités manquées → get_missed_upgrades
11. Explicitly says "briefing", "weekly report", "résumé", or "full report" → get_briefing
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

_DRAFT_SYSTEM = """You are helping a cycling club owner write a short, natural WhatsApp message to one of their athletes.

Style:
- Warm, genuine, friendly — like a message from someone who actually knows them
- 2 sentences maximum. No fluff, no corporate tone
- Start with "Hey [FirstName],"
- Never mention ClubRide.Ai or the owner's name
- Sound like the owner noticed something specific, not like a mass message

Content:
- Weave in exactly ONE key number from the data provided (marked as "Key number") — mention it naturally, not robotically
- End with a single clear, low-pressure call to action:
  * upgrade → come by to see new bikes ("worth a look", "pop by")
  * service → book the bike in ("before it gets worse", "before summer")
  * ghost → coffee at the shop, no pressure ("whenever you're free")
  * engagement → open invitation to stop by
- If the owner's note has extra instructions, honour them naturally"""


# ── Language helper ────────────────────────────────────────────────────────────

def _with_lang(system: str, lang: str) -> str:
    """Append a language instruction to a Gemini system prompt."""
    return f"{system}\n\n{t('llm_lang', lang)}"


# ── Formatters (deterministic, zero Gemini) ────────────────────────────────────

def _fmt_leaderboard(club_id: int, top_n: int = 10, lang: str = "en") -> str:
    from .retriever import _norm, _load_csv, _paths
    import pandas as pd

    data     = get_leaderboard(club_id, top_n=200)
    athletes = data.get("athletes", [])
    week, year = data.get("week"), data.get("year")
    if not athletes:
        return t("lb_no_data", lang)

    p      = _paths(club_id)
    enr_df = _load_csv(p["enriched"])
    community_norms = set()
    if not enr_df.empty:
        for name in enr_df["Athlete_Raw"].dropna():
            community_norms.add(_norm(str(name)))
        for name in enr_df["Matched_Name"].dropna():
            community_norms.add(_norm(str(name)))

    from .retriever import _NO_SIGNAL
    community = [a for a in athletes
                 if _norm(a["Athlete"]) in community_norms
                 and _norm(a["Athlete"]) not in _NO_SIGNAL]
    shown     = community[:top_n]

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
        subtitle.append(f"{past_count} {t('lb_came', lang)}")
    if future_count:
        upcoming = next((f"{d.strftime('%A %b %d')}" for d, _t, f in evt_dates if f), "this week")
        subtitle.append(f"{future_count} {t('lb_registered', lang)} {upcoming}")
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
        return t("lb_no_community", lang, week=week, year=year)

    return "\n".join(lines)


def _fmt_service(club_id: int, alert_type: str = "service", lang: str = "en") -> str:
    sections = []

    if alert_type in ("service", "both"):
        alerts = get_service_due(club_id, limit=10)
        if alerts:
            lines = [t("svc_due_header", lang, n=len(alerts)) + "\n"]
            for a in alerts:
                km       = float(a.get("Km_Since_Service", 0) or 0)
                bike     = a.get("Bike_Model") or a.get("Bike_Brand") or "unknown"
                last     = str(a.get("Last_Service_Date", "") or "")
                last_str = f" · last: {last[:7]}" if last and last != "nan" else ""
                lines.append(f"• {a['Athlete']} — {km:,.0f}{t('svc_since', lang)}{last_str} · {bike}")
            sections.append("\n".join(lines))

    if alert_type in ("chain", "both", "service"):
        data     = get_chain_alerts(club_id, limit=10)
        athletes = data.get("athletes", [])
        if athletes:
            lines = [t("chain_due_header", lang, n=len(athletes)) + "\n"]
            for a in athletes:
                km   = float(a.get("Km_Since_Chain", 0) or 0)
                bike = a.get("Bike_Model") or a.get("Bike_Brand") or "unknown"
                lines.append(f"• {a['Athlete']} — {km:,.0f}{t('chain_since', lang)} · {bike}")
            sections.append("\n".join(lines))

    if not sections:
        return t("svc_no_alerts", lang)

    return "\n".join(sections)


def _fmt_upgrade(club_id: int, limit: int = 8, lang: str = "en") -> str:
    data       = get_upgrade_candidates(club_id, limit=limit)
    candidates = data.get("candidates", [])
    if not candidates:
        return t("upg_none", lang)

    candidates = sorted(candidates,
                        key=lambda c: (-int(c.get("events_count") or 0),
                                       -float(c.get("weekly_km") or 0)))
    lines = [t("upg_header", lang, n=len(candidates)) + "\n"]

    for c in candidates:
        wk     = float(c.get("weekly_km") or 0)
        km_src = c.get("km_source", "real")
        wk_str = t("upg_km_wk_est", lang, wk=wk) if km_src == "estimated" else t("upg_km_wk", lang, wk=wk)

        bike      = c.get("primary_bike") or ""
        bk_src    = c.get("bike_source", "unknown")
        has_bike  = bool(bike and bike not in ("nan", "unknown", ""))
        if not has_bike:
            bike_str = t("upg_bike_unk", lang)
        elif bk_src == "inferred":
            bike_str = f"{bike} (inferred)"
        else:
            bike_str = bike

        pt = c.get("primary_tier") or ""
        tier_str = f"({pt} tier)" if pt and pt not in ("nan", "unknown", "") else ""

        bike_km  = float(c.get("primary_bike_km") or 0)
        km_str   = f" · {t('upg_km_on_bike', lang, km=bike_km)}" if bk_src == "real" and bike_km > 0 else ""

        spd = float(c.get("speed_est") or 0)
        spd_str = f" · {spd:.1f}km/h avg" if spd > 0 and not c.get("has_profile") else ""

        src     = c.get("purchase_source", "")
        yr      = c.get("purchase_year", "")
        src_str = f" · {t('upg_bought_tnce', lang, yr=yr)}" if src == "Club TNCE" and yr else \
                  f" · {t('upg_bought_ext', lang)}" if src else ""

        ev = int(c.get("events_count") or 0)
        if ev == 0:    loyalty = t("upg_solo", lang)
        elif ev < 4:   loyalty = t("upg_occasional", lang, n=ev)
        elif ev < 10:  loyalty = t("upg_regular", lang, n=ev)
        else:          loyalty = t("upg_loyal", lang, n=ev)

        lines.append(
            f"• {c['name']} — {wk_str}"
            f" · {bike_str} {tier_str}{km_str}{spd_str}"
            f" · {loyalty}{src_str}"
        )
    return "\n".join(lines)


def _fmt_weekend_priorities(club_id: int, lang: str = "en") -> str:
    data    = get_weekend_priorities(club_id)
    upgrade = data.get("upgrade")
    service = data.get("service")

    if not upgrade and not service:
        return t("wp_none", lang)

    lines = [t("wp_header", lang) + "\n"]

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
        draft    = _draft_whatsapp(upgrade["name"], "upgrade", lang,
                                   weekly_km=wk, bike=bike, events=ev)
        lines.append(
            f"🚲 *{upgrade['name']}*\n"
            f"   Upgrade window · {wk:.0f}km/wk"
            f" · {bike}{km_str}{src_str} · {ev} events\n"
            f"   📱 _{draft}_"
        )

    if service:
        km_since = float(service.get("km_since") or 0)
        draft    = _draft_whatsapp(service["name"], "service", lang, km_since=km_since)
        lines.append(
            f"\n🔧 *{service['name']}*\n"
            f"   Service overdue · {km_since:,.0f}km since last\n"
            f"   📱 _{draft}_"
        )

    return "\n".join(lines)


def _fmt_recruits(club_id: int, limit: int = 10, lang: str = "en") -> str:
    data     = get_potential_recruits(club_id, limit=limit)
    recruits = data.get("recruits", [])
    if not recruits:
        return t("rec_none", lang)

    lines = [t("rec_header", lang, n=len(recruits)) + "\n"]
    for r in recruits:
        bike = r.get("primary_bike") or ""
        bike = t("rec_no_bike", lang) if not bike or bike == "nan" else bike
        loc  = r.get("location", "")
        loc  = "" if not loc or loc == "nan" else loc
        lines.append(
            f"• {r['name']} — {t('rec_km_wk', lang, km=r['weekly_km'])}"
            f" · {bike} ({t('rec_tier', lang, tier=r['primary_tier'])})"
            f" · {t('rec_rider', lang, tier=r['rider_tier'])}"
            + (f" · {loc}" if loc else "")
        )
    lines.append(t("rec_footer", lang))
    return "\n".join(lines)


def _fmt_at_risk(club_id: int, lang: str = "en") -> str:
    data    = get_at_risk_members(club_id)
    members = data.get("at_risk", [])
    if not members:
        return t("atrisk_none", lang)
    lines = [
        t("atrisk_header", lang, n=len(members)),
        t("atrisk_sub", lang) + "\n",
    ]
    for m in members:
        w      = m["weeks_absent"]
        rides  = m["attended"]
        flag   = " ❗" if w >= 12 else ""
        lines.append(
            f"• {m['name']} — {rides} {t('rides_together', lang)} · {t('atrisk_absent', lang, w=w)}{flag}"
        )
    lines.append(t("atrisk_footer", lang))
    return "\n".join(lines)


def _draft_whatsapp(name: str, signal: str, lang: str = "en",
                    hint: str = "", **kwargs) -> str:
    def _approx_km(km: float) -> str:
        if km >= 30_000: return f"over {int(km/10_000)*10}k km"
        if km >= 10_000: return f"around {round(km/5_000)*5}k km"
        if km >= 5_000:  return f"nearly {round(km/1_000)}k km"
        return "a few thousand km"

    def _approx_wk(wk: float) -> str:
        if wk < 20: return "regularly active"
        return f"around {round(wk/20)*20}km a week"

    def _approx_weeks(w: int) -> str:
        if w >= 16: return f"about {round(w/4)} months"
        if w >= 8:  return "a couple of months"
        return "a few weeks"

    first = name.split()[0]
    if signal == "service":
        km   = float(kwargs.get("km_since", 0) or 0)
        bike = kwargs.get("bike", "your bike")
        ctx  = (f"Athlete first name: {first}\n"
                f"Bike: {bike}\n"
                f"Key number: {_approx_km(km)} since last service\n"
                f"Reason: bike is overdue for a service — mention this naturally, not as a data readout")
    elif signal == "upgrade":
        wk      = float(kwargs.get("weekly_km", 0) or 0)
        bike    = kwargs.get("bike", "their current bike")
        bike_km = float(kwargs.get("bike_km", 0) or 0)
        ev      = int(kwargs.get("events", 0) or 0)
        key_num = f"{_approx_km(bike_km)} on their {bike}" if bike_km >= 20_000 else _approx_wk(wk)
        ctx  = (f"Athlete first name: {first}\n"
                f"Current bike: {bike}\n"
                f"Key number: {key_num}\n"
                f"Reason: serious rider who has put a lot into their bike — time for an upgrade conversation")
    elif signal == "ghost":
        weeks = int(kwargs.get("weeks", 0) or 0)
        ev    = int(kwargs.get("events", 0) or 0)
        ctx   = (f"Athlete first name: {first}\n"
                 f"Key number: haven't seen them for {_approx_weeks(weeks)}\n"
                 f"Reason: loyal member who has stopped showing up — keep it warm, no pressure")
    else:
        wk  = float(kwargs.get("weekly_km", 0) or 0)
        ev  = int(kwargs.get("events", 0) or 0)
        ctx = (f"Athlete first name: {first}\n"
               f"Key number: {_approx_wk(wk)}\n"
               f"Reason: general re-engagement")
    if hint:
        ctx += f"\nOwner's note: {hint}"
    try:
        resp = _client.models.generate_content(
            model="gemini-2.5-flash",
            contents=ctx,
            config=types.GenerateContentConfig(
                system_instruction=_with_lang(_DRAFT_SYSTEM, lang),
                temperature=0.7,
            )
        )
        return resp.text.strip()
    except Exception:
        km = float(kwargs.get("km_since", 0) or 0)
        wk = float(kwargs.get("weekly_km", 0) or 0)
        if signal == "service":
            return t("draft_fallback_svc", lang, first=first, km=km)
        if signal == "upgrade":
            return t("draft_fallback_upg", lang, first=first)
        if signal == "ghost":
            return t("draft_fallback_ghost", lang, first=first)
        return t("draft_fallback_generic", lang, first=first)


def _handle_draft_message(club_id: int, athlete_name: str,
                          lang: str = "en", question: str = "",
                          owner_id: str = "") -> str:
    p = get_athlete_profile(club_id, athlete_name)

    if p.get("data_quality") == "ambiguous":
        matches = p.get("matches", [])
        names   = "\n".join(f"• {n}" for n in matches)
        return t("draft_ambiguous", lang, n=len(matches), name=athlete_name, names=names)

    if not p.get("found"):
        return t("draft_not_found", lang, name=athlete_name)

    name    = p.get("name", athlete_name)
    if owner_id and name:
        set_last_athlete(owner_id, name)
    svc_due = p.get("service_due", False)
    chn_due = p.get("chain_due", False)
    svc_km  = float(p.get("km_since_service", 0) or 0)
    chn_km  = float(p.get("km_since_chain", 0) or 0)
    rtier   = p.get("rider_tier", "")
    btier   = p.get("primary_tier", "")

    _KNOWN_BRANDS = {
        "canyon","trek","specialized","bmc","scott","cannondale","giant",
        "pinarello","colnago","bianchi","cervelo","look","orbea","merida",
        "wilier","felt","factor","lapierre","focus","cube","ridley","rose",
    }
    raw_bike  = p.get("primary_bike") or ""
    raw_brand = p.get("primary_brand") or ""
    raw_brand = "" if raw_brand in ("nan", "unknown") else raw_brand
    raw_bike  = "" if raw_bike  in ("nan", "unknown") else raw_bike
    if raw_brand and raw_brand.lower() in _KNOWN_BRANDS:
        bike = raw_brand
    elif any(b in raw_bike.lower() for b in _KNOWN_BRANDS):
        bike = raw_bike.split()[0].capitalize()
    else:
        bike = "your bike"

    wk = float(p.get("weekly_km", 0) or 0)
    ev = p.get("events_count", 0)

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

    if ev == 0:
        first = name.split()[0]
        return t("draft_no_events", lang, first=first)

    bike_km    = float(p.get("primary_bike_km", 0) or 0)
    is_upgrade = (
        (rtier in ("top", "mid") and btier in ("mid", "entry")) or
        (bike_km >= 25_000 and btier not in ("top", "unknown"))
    )

    first = name.split()[0]
    if is_upgrade:
        draft  = _draft_whatsapp(name, "upgrade", lang, hint=question,
                                 weekly_km=wk, bike=bike, events=ev, bike_km=bike_km)
        reason = t("draft_reason_upg", lang, wk=wk, bk_km=bike_km, bike=bike)
    elif svc_due:
        draft  = _draft_whatsapp(name, "service", lang, hint=question, km_since=svc_km, bike=bike)
        reason = t("draft_reason_svc", lang, km=svc_km)
    elif chn_due:
        draft  = _draft_whatsapp(name, "service", lang, hint=question, km_since=chn_km, bike=bike)
        reason = t("draft_reason_chn", lang, km=chn_km)
    elif is_at_risk:
        draft  = _draft_whatsapp(name, "ghost", lang, hint=question,
                                 weeks=weeks_absent, events=ev)
        reason = t("draft_reason_risk", lang, w=weeks_absent)
    else:
        draft  = _draft_whatsapp(name, "engagement", lang, hint=question,
                                 weekly_km=wk, events=ev)
        reason = t("draft_reason_eng", lang)

    return t("draft_header", lang, first=first, reason=reason, draft=draft) + t("draft_footer", lang)


def _fmt_loyal(club_id: int, limit: int = 10, lang: str = "en") -> str:
    from .retriever import get_loyal_members
    data    = get_loyal_members(club_id, limit=limit)
    members = data.get("loyal", [])
    if not members:
        return t("loyal_none", lang)
    curr_year = date.today().year
    lines = [
        t("loyal_header", lang, n=len(members)),
        t("loyal_sub", lang) + "\n",
    ]
    for m in members:
        yr_str   = f" · {m['curr_year_km']:,.0f}km in {curr_year}" if m["curr_year_km"] > 0 else ""
        bike_str = f" · {m['primary_bike']}" if m["primary_bike"] else ""
        tier_str = t("loyal_rider_tier", lang, tier=m["rider_tier"]) \
                   if m["rider_tier"] and m["rider_tier"] not in ("unknown", "") else ""
        lines.append(
            f"• {m['name']} — {t('loyal_rides', lang, n=m['events'])}{yr_str}{bike_str}{tier_str}"
        )
    return "\n".join(lines)


def _fmt_missed_upgrades(club_id: int, lang: str = "en") -> str:
    data     = get_missed_upgrades(club_id)
    upgrades = data.get("upgrades", [])
    if not upgrades:
        return t("missed_none", lang)

    lines = [
        t("missed_header", lang, n=len(upgrades)),
        t("missed_sub", lang) + "\n",
    ]
    for u in upgrades:
        new_label = t("missed_new", lang)
        was_label = t("missed_was", lang)
        source = "✓" if u.get("confirmed") else "est."
        lines.append(
            f"⬆️ *{u['name']}* · {u['weekly_km']:.0f} km/wk\n"
            f"  {new_label}: {u['new_bike']} · {u['new_tier']} · "
            f"{u['new_km']:,.0f}km · {source} {u['purchase_month']}\n"
            f"  {was_label}: {u['old_bike']} · {u['old_tier']} · {u['old_km']:,.0f}km"
        )
    return "\n".join(lines)


_weather_cache: dict = {}

def _fetch_weather(lat: float, lon: float) -> dict:
    """Fetch 7-day forecast from Open-Meteo (free, no key). Cached 3 hours."""
    import time as _time
    cache_key = f"{lat},{lon}"
    if cache_key in _weather_cache:
        cached = _weather_cache[cache_key]
        if _time.time() - cached["ts"] < 10800:
            return cached["data"]

    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":   lat,
                "longitude":  lon,
                "daily":      "temperature_2m_max,temperature_2m_min,precipitation_sum,wind_speed_10m_max,precipitation_probability_max",
                "timezone":   "Europe/Zurich",
                "forecast_days": 14,
            },
            timeout=5,
        )
        r.raise_for_status()
        daily = r.json().get("daily", {})
        result = {}
        for i, dt in enumerate(daily.get("time", [])):
            tmax  = daily.get("temperature_2m_max",             [None])[i]
            tmin  = daily.get("temperature_2m_min",             [None])[i]
            rain  = daily.get("precipitation_sum",              [0])[i] or 0
            wind  = daily.get("wind_speed_10m_max",             [0])[i] or 0
            prob  = daily.get("precipitation_probability_max",  [0])[i] or 0
            icon  = "🌧" if prob >= 50 else ("🌦" if prob >= 25 else "☀️")
            result[dt] = {
                "tmax": round(tmax) if tmax is not None else None,
                "tmin": round(tmin) if tmin is not None else None,
                "rain_mm": round(rain, 1),
                "wind_kmh": round(wind),
                "prob": int(prob),
                "icon": icon,
            }
        _weather_cache[cache_key] = {"ts": _time.time(), "data": result}
        return result
    except Exception:
        return {}


def _fmt_briefing(club_id: int, lang: str = "en") -> str:
    from .retriever import _norm, _load_csv, _paths, _load_leaderboard, _load_attendance, _EXCLUDED_ATHLETES
    from pathlib import Path as _Path
    import pandas as _pd

    sections = []
    p       = _paths(club_id)
    enr_df  = _load_csv(p["enriched"])
    prof_df = _load_csv(p["profiles"])
    att_df  = _load_attendance(club_id)

    community = set()
    if not enr_df.empty:
        for name in enr_df["Athlete_Raw"].dropna():
            n = _norm(str(name))
            if n not in _EXCLUDED_ATHLETES:
                community.add(n)

    lb_df = _load_leaderboard(club_id)
    week = year = None
    top3_year = []
    if not lb_df.empty:
        latest_year = lb_df["Year"].max()
        latest_week = lb_df[lb_df["Year"] == latest_year]["Week_Number"].max()
        week, year  = int(latest_week), int(latest_year)

        if not prof_df.empty:
            prof_df["CurrYear_km"] = _pd.to_numeric(prof_df["CurrYear_km"], errors="coerce").fillna(0)
            prof_df["_norm_tmp"]   = prof_df["Name"].apply(_norm)
            yr_comm = (prof_df[prof_df["_norm_tmp"].isin(community) &
                               ~prof_df["_norm_tmp"].isin(_EXCLUDED_ATHLETES)]
                       .sort_values("CurrYear_km", ascending=False)
                       .head(3))
            top3_year = [(str(r["Name"]), round(float(r["CurrYear_km"]), 0))
                         for _, r in yr_comm.iterrows() if float(r["CurrYear_km"]) > 0]

    events_year = 0
    if not att_df.empty:
        try:
            att_df["_date"] = _pd.to_datetime(att_df["Date"], format="mixed",
                                               dayfirst=False, errors="coerce")
            events_year = int(att_df[att_df["_date"].dt.year == date.today().year].shape[0])
        except Exception:
            pass

    profiled_count = 0
    bikes_known    = 0
    if not prof_df.empty:
        prof_df["_norm"] = prof_df["Name"].apply(_norm)
        comm_prof_tmp  = prof_df[prof_df["_norm"].isin(community)]
        profiled_count = len(comm_prof_tmp)
        bikes_known    = int(comm_prof_tmp[
            comm_prof_tmp["primary_bike"].notna() &
            ~comm_prof_tmp["primary_bike"].isin(["nan", "unknown", ""])
        ].shape[0])

    # Attendance date range — "since Jun 2025"
    att_since = ""
    if not enr_df.empty:
        try:
            min_date = _pd.to_datetime(enr_df["Date"], format="mixed",
                                       dayfirst=False, errors="coerce").min()
            if not _pd.isna(min_date):
                att_since = f" (since {min_date.strftime('%b %Y')})"
        except Exception:
            pass

    # Profile scrape date — "data: May 13"
    prof_date = ""
    if not prof_df.empty and "Scraped_At" in prof_df.columns:
        try:
            latest_scrape = _pd.to_datetime(prof_df["Scraped_At"],
                                            errors="coerce").max()
            if not _pd.isna(latest_scrape):
                prof_date = f" (data: {latest_scrape.strftime('%b %d')})"
        except Exception:
            pass

    medals   = ["🥇", "🥈", "🥉"]
    top3_str = "  ".join(f"{medals[i]} {n} ({km:,.0f}km)" for i, (n, km) in enumerate(top3_year))
    header   = (
        f"*🚴 TNCE Briefing · W{week}/{year}*\n"
        f"{events_year} {t('briefing_stat_events', lang)} · "
        f"{len(community)} {t('briefing_stat_members', lang)}{att_since} · "
        f"{profiled_count} {t('briefing_stat_profiles', lang)}{prof_date} · "
        f"{bikes_known} {t('briefing_stat_bikes', lang)}\n"
        + (f"{t('briefing_top_year', lang, year=date.today().year)}{prof_date} {top3_str}"
           if top3_str else t("briefing_no_year", lang))
    )
    sections.append(header)

    if not prof_df.empty:
        comm_prof  = prof_df[prof_df["_norm"].isin(community)]
        bikes_path = _Path(p["profiles"]).parent / "athlete_bikes.csv"
        try:
            bk       = _pd.read_csv(bikes_path, dtype=str)
            comm_ids = comm_prof["Athlete_ID"].astype(str).tolist()
            comm_bk  = bk[bk["Athlete_ID"].astype(str).isin(comm_ids)]
            riders_per_brand = (comm_bk.groupby("Brand")["Athlete_ID"]
                                .nunique()
                                .sort_values(ascending=False)
                                .head(6))
            if not riders_per_brand.empty:
                total_p    = len(comm_ids)
                brand_parts = []
                for i, (b, n) in enumerate(riders_per_brand.items()):
                    entry = f"{b} {n/total_p*100:.0f}%({n})"
                    brand_parts.append(f"*{entry}*" if i == 0 else entry)
                brand_str = " · ".join(brand_parts)
                sections.append(f"{t('briefing_brands', lang)} \n{brand_str}")
        except Exception:
            pass

    if not att_df.empty and "_date" in att_df.columns:
        try:
            today_ts  = _pd.Timestamp(date.today())
            upcoming  = (att_df[att_df["_date"] >= today_ts]
                         .sort_values("_date").head(3))
            if not upcoming.empty:
                try:
                    cfg_path = ROOT / "config.json"
                    cfg      = json.loads(cfg_path.read_text())
                    lat      = cfg["club"].get("fallback_lat", 46.5197)
                    lon      = cfg["club"].get("fallback_lon", 6.6323)
                except Exception:
                    lat, lon = 46.5197, 6.6323

                forecast  = _fetch_weather(lat, lon)
                horizon   = date.today() + timedelta(days=14)

                lines = [t("briefing_upcoming", lang)]
                for _, ev in upcoming.iterrows():
                    d     = ev["_date"].strftime("%a %b %d")
                    dist  = str(ev.get("Distance",      "") or "").strip()
                    elev  = str(ev.get("Elevation",     "") or "").strip()
                    count = str(ev.get("Athletes_Count", "") or "").strip()
                    route = ""
                    if dist  and dist  not in ("nan", ""):
                        route += f" · {dist}"
                    if elev  and elev  not in ("nan", ""):
                        route += f" · ↑{elev}"
                    if count and count not in ("nan", "0", ""):
                        route += f" · 👥 {count} registered"

                    weather  = ""
                    ev_date  = ev["_date"].date()
                    if ev_date <= horizon and str(ev_date) in forecast:
                        w       = forecast[str(ev_date)]
                        t_str   = f"{w['tmin']}–{w['tmax']}°C" if w["tmin"] is not None else f"{w['tmax']}°C"
                        w_str   = f" · {w['wind_kmh']}km/h wind"
                        weather = f" · {w['icon']} {t_str}{w_str}"
                    elif ev_date > horizon:
                        weather = " · (forecast not yet available)"

                    lines.append(f"• {d} — {ev['Title']}{route}{weather}")
                sections.append("\n".join(lines))
        except Exception:
            pass

    # ── Smart builder — trims variable sections to fit Twilio limit ──────────
    from .retriever import get_loyal_members

    CHAR_LIMIT = 1480

    def _row_loyal(m):
        yr = f" · {m['curr_year_km']:,.0f}km" if m["curr_year_km"] > 0 else ""
        bk = f" · {m['primary_bike']}" if m["primary_bike"] else ""
        return f"• {m['name']} — {t('loyal_rides', lang, n=m['events'])}{yr}{bk}"

    def _row_upgrade(c):
        bike = c.get("primary_bike") or t("upg_bike_unk", lang)
        bike = t("upg_bike_unk", lang) if bike in ("nan", "unknown", "") else bike
        return f"• {c['name']} — {t('upg_km_wk', lang, wk=c['weekly_km'])} · {bike}"

    def _row_service(a):
        km   = float(a.get("Km_Since_Service", 0) or 0)
        bike = a.get("Bike_Model") or a.get("Bike_Brand") or "?"
        return f"• {a['Athlete']} — {km:,.0f}{t('svc_since', lang)} · {bike}"

    def _row_atrisk(m):
        flag = " ❗" if m["weeks_absent"] >= 12 else ""
        return f"• {m['name']} — {t('loyal_rides', lang, n=m['attended'])} · {t('atrisk_absent', lang, w=m['weeks_absent'])}{flag}"

    def _assemble(loyal, upg, svc, risk):
        parts = []
        if loyal:
            rows = [t("briefing_loyal_header", lang)] + [_row_loyal(m) for m in loyal]
            parts.append("\n".join(rows))
        if upg:
            rows = [t("briefing_upg_header", lang, n=len(upg))] + [_row_upgrade(c) for c in upg]
            parts.append("\n".join(rows))
        else:
            parts.append(t("briefing_upg_none", lang))
        if svc:
            rows = [t("briefing_svc_header", lang, n=len(svc))] + [_row_service(a) for a in svc]
            parts.append("\n".join(rows))
        else:
            parts.append(t("briefing_svc_clear", lang))
        if risk:
            rows = [t("briefing_risk_header", lang, n=len(risk))] + [_row_atrisk(m) for m in risk]
            parts.append("\n".join(rows))
        else:
            parts.append(t("briefing_risk_ok", lang))
        return "\n".join(parts)

    loyal_all = get_loyal_members(club_id, limit=5).get("loyal", [])
    upg_all   = get_upgrade_candidates(club_id, limit=5).get("candidates", [])
    svc_all   = get_service_due(club_id, limit=5)
    risk_all  = get_at_risk_members(club_id).get("at_risk", [])

    fixed = "\n".join(sections)
    ln, rn = len(loyal_all), len(risk_all)

    while True:
        variable = _assemble(loyal_all[:ln], upg_all, svc_all, risk_all[:rn])
        if len(fixed) + len(variable) <= CHAR_LIMIT or (ln <= 2 and rn <= 2):
            break
        if rn > 2:
            rn -= 1
        elif ln > 2:
            ln -= 1
        else:
            break

    return fixed + variable


# ── Athlete profile — deterministic, facts only ───────────────────────────────

def _handle_athlete(club_id: int, athlete_name: str,
                    question: str, history: list[dict],
                    lang: str = "en", owner_id: str = "") -> str:
    p = get_athlete_profile(club_id, athlete_name)

    if p.get("data_quality") == "ambiguous":
        matches = p.get("matches", [])
        names   = "\n".join(f"• {n}" for n in matches)
        return t("profile_ambiguous", lang, n=len(matches), name=athlete_name, names=names)

    if not p.get("found"):
        return t("profile_not_found", lang, name=athlete_name)

    name   = p.get("name", athlete_name)
    if owner_id and name:
        set_last_athlete(owner_id, name)
    ev     = p.get("events_count", 0)
    first  = p.get("first_seen", "")
    last   = p.get("last_seen", "")

    if p.get("data_quality") == "attendance_only":
        lines = [f"*{name}*"]
        lines.append(f"Events: {ev}  ·  First: {first}  ·  Last: {last}")
        lines.append(t("profile_att_footer", lang))
        return "\n".join(lines)

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

    stat_parts = []
    if wk > 0:      stat_parts.append(t("profile_km_wk",       lang, wk=wk))
    if alltime > 0: stat_parts.append(t("profile_km_lifetime",  lang, km=alltime))
    if speed > 0:   stat_parts.append(t("profile_kmh_avg",      lang, sp=speed))
    if longest > 0: stat_parts.append(t("profile_longest",      lang, km=longest))
    if stat_parts:
        lines.append("  ·  ".join(stat_parts))

    purchase_str = ""
    if pd_str and len(pd_str) >= 7:
        try:
            from datetime import datetime as _dt2
            pd_date2     = _dt2.strptime(pd_str[:7], "%Y-%m")
            purchase_str = pd_date2.strftime("%b %Y")
        except Exception:
            pass

    bike_parts = []
    if bike:     bike_parts.append(bike)
    if btier:    bike_parts.append(f"{btier} tier")
    if bike_km > 0: bike_parts.append(t("profile_km_on_bike", lang, km=bike_km))
    if src and purchase_str:
        bike_parts.append(t("profile_bought", lang, src=src, date=purchase_str))
    elif src:
        bike_parts.append(t("profile_bought_nodate", lang, src=src))
    if bike_parts:
        lines.append("  ·  ".join(bike_parts))

    ev_line = t("profile_events", lang, n=ev)
    if first: ev_line += f"  ·  {t('profile_first', lang, date=first)}"
    if last:
        try:
            last_date  = date.fromisoformat(str(last)[:10])
            last_label = t("profile_upcoming", lang) if last_date > date.today() \
                         else t("profile_last", lang)
        except Exception:
            last_label = t("profile_last", lang)
        ev_line += f"  ·  {last_label} {last}"
    lines.append(ev_line)

    alerts = []
    if svc_due: alerts.append(t("profile_svc_alert", lang, km=svc_km))
    if chn_due: alerts.append(t("profile_chn_alert", lang, km=chn_km))
    if alerts:
        lines.append("⚠️ " + "  ·  ".join(alerts))

    if rtier == "top" and btier in ("mid", "entry"):
        lines.append(t("profile_upg_top", lang))
    elif rtier == "mid" and btier == "entry":
        lines.append(t("profile_upg_mid", lang))

    return "\n".join(lines)


# ── Tool executor ──────────────────────────────────────────────────────────────

def _execute_tool(tool_name: str, args: dict,
                  club_id: int, question: str,
                  history: list[dict], lang: str = "en",
                  owner_id: str = "") -> str:
    if tool_name == "get_leaderboard":
        return _fmt_leaderboard(club_id, top_n=int(args.get("top_n", 10)), lang=lang)

    if tool_name == "get_athlete_profile":
        return _handle_athlete(club_id, args.get("athlete_name", ""), question, history,
                               lang=lang, owner_id=owner_id)

    if tool_name == "draft_message":
        return _handle_draft_message(club_id, args.get("athlete_name", ""), lang=lang,
                                     question=question, owner_id=owner_id)

    if tool_name == "get_upgrade_candidates":
        return _fmt_upgrade(club_id, limit=int(args.get("limit", 8)), lang=lang)

    if tool_name == "get_service_alerts":
        return _fmt_service(club_id, alert_type=args.get("type", "service"), lang=lang)

    if tool_name == "get_at_risk_members":
        return _fmt_at_risk(club_id, lang=lang)

    if tool_name == "get_weekend_priorities":
        return _fmt_weekend_priorities(club_id, lang=lang)

    if tool_name == "get_potential_recruits":
        return _fmt_recruits(club_id, limit=int(args.get("limit", 10)), lang=lang)

    if tool_name == "get_loyal_members":
        return _fmt_loyal(club_id, limit=int(args.get("limit", 10)), lang=lang)

    if tool_name == "get_missed_upgrades":
        return _fmt_missed_upgrades(club_id, lang=lang)

    if tool_name == "get_briefing":
        return _fmt_briefing(club_id, lang=lang)

    if tool_name == "show_help":
        return t("help", lang)

    return t("help", lang)


# ── Main entry point ───────────────────────────────────────────────────────────

def handle(message: str, owner_id: str,
           club_id: int = CLUB_ID) -> str:
    history = get_history(owner_id)
    lang    = get_lang(owner_id)

    import re

    # ── Resolve partial name from session context ─────────────────────────────
    # If user says "draft for Simon" after previously resolving "Simon Rimaz",
    # substitute the full name so the bot doesn't ask for disambiguation again.
    last_athlete = get_last_athlete(owner_id)
    if last_athlete:
        first = last_athlete.split()[0].lower()
        _m = message.lower()
        if (first in _m and last_athlete.lower() not in _m
                and re.search(r'\b' + re.escape(first) + r'\b', _m)):
            message = re.sub(
                r'\b' + re.escape(first) + r'\b',
                last_athlete,
                message,
                count=1,
                flags=re.IGNORECASE
            )

    # Help
    if message.strip().lower() in ("help", "aide", "?", "commands", "menu"):
        reply = t("help", lang)
        add_turn(owner_id, message, reply)
        return reply

    # ── Language toggle ───────────────────────────────────────────────────────
    _LANG_EXACT = {
        "lang fr": "fr",
        "lang en": "en",
    }
    _LANG_CONTAINS = {
        "fr": {"switch to french", "passe en français", "passe en francais"},
        "en": {"switch to english", "passe en anglais"},
    }
    _msg_lower = message.strip().lower()
    _toggled = _LANG_EXACT.get(_msg_lower)
    if not _toggled:
        for _target, _phrases in _LANG_CONTAINS.items():
            if any(p in _msg_lower for p in _phrases):
                _toggled = _target
                break
    if _toggled:
        set_lang(owner_id, _toggled)
        lang  = _toggled
        reply = t(f"lang_set_{_toggled}", lang)
        add_turn(owner_id, message, reply)
        return reply

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
            reply = t("feedback_logged", lang, action=action, athlete=athlete)
        else:
            reply = t("feedback_no_context", lang)
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
        contents = message

    # ── Keyword fallback — works even when Gemini is 503 ─────────────────────
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
        "loyal":             ("get_loyal_members", {}),
        "top members":       ("get_loyal_members", {}),
        "missed":            ("get_missed_upgrades", {}),
        "missed opportunity":("get_missed_upgrades", {}),
        "opportunit":        ("get_missed_upgrades", {}),
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
                idx  = msg_low.index(kw) + len(kw)
                name = message[idx:].strip().strip("?.,!")
                if not name:
                    continue
                args = {"athlete_name": name}
            try:
                reply = _execute_tool(tool, args, club_id, message, history, lang=lang, owner_id=owner_id)
            except Exception as e:
                print(f"  Keyword fallback error [{tool}]: {e}")
                reply = t("error", lang)
            add_turn(owner_id, message, reply)
            return reply

    # ── Privacy gate — anonymise names before sending to Gemini ──────────────
    try:
        from .retriever import _paths, _load_csv
        _df    = _load_csv(_paths(club_id)["profiles"])
        _names = _df["Name"].dropna().tolist() if not _df.empty else []
    except Exception:
        _names = []

    _anon = build_anonymiser(_names)

    if isinstance(contents, list):
        anon_contents = []
        for turn in contents:
            anon_parts = [types.Part(text=_anon.anonymise(p.text))
                          if hasattr(p, "text") else p
                          for p in turn.parts]
            anon_contents.append(types.Content(role=turn.role, parts=anon_parts))
    else:
        anon_contents = _anon.anonymise(contents)

    anon_msg = anon_contents if isinstance(anon_contents, str) else anon_contents[-1].parts[0].text
    print(f"\n[PRIVACY GATE]")
    print(f"  Original : {message}")
    print(f"  To Gemini: {anon_msg}")

    # Gemini picks the tool — 3 retries with 2s backoff on 503
    import time as _time
    response = None
    for attempt in range(3):
        try:
            response = _client.models.generate_content(
                model="gemini-2.5-flash",
                contents=anon_contents,
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
            reply = t("busy", lang)
            add_turn(owner_id, message, reply)
            return reply

    if response is None:
        reply = t("busy", lang)
        add_turn(owner_id, message, reply)
        return reply

    # Extract function call — de-anonymise args before tool execution
    _content = response.candidates[0].content if response.candidates else None
    _parts   = (_content.parts if _content else None) or []
    for part in _parts:
        if hasattr(part, "function_call") and part.function_call:
            fn        = part.function_call
            raw_args  = dict(fn.args)
            args      = _anon.deanonymise_args(raw_args)
            if raw_args != args:
                print(f"  Gemini got: {fn.name}({raw_args})")
                print(f"  Resolved  : {fn.name}({args})")
            # Extract and persist language detected by Gemini
            detected_lang = args.pop("language", None)
            if detected_lang in ("en", "fr") and detected_lang != lang:
                set_lang(owner_id, detected_lang)
                lang = detected_lang
                print(f"  Lang → {lang} (Gemini detected)")
            try:
                reply = _execute_tool(fn.name, args, club_id, message, history, lang=lang, owner_id=owner_id)
            except Exception:
                reply = t("error", lang)
            add_turn(owner_id, message, reply)
            return reply

    # No tool call — show the command menu
    reply = t("help", lang)
    add_turn(owner_id, message, reply)
    return reply
