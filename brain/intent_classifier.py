"""
brain/intent_classifier.py
Deterministic message router — classifies every owner WhatsApp message.

Priority order (no Gemini until Step 4):
  1. Feedback reply    → feedback.py keyword map (zero Gemini)
  2. Known command     → EN + FR keyword map (zero Gemini)
  3. Athlete lookup    → name extraction (zero Gemini)
     └─ No name found  → intent: clarify (ask owner, zero Gemini)
     └─ Two+ names     → intent: clarify (ask which comparison)
  4. Free-text         → Gemini (last resort only)

Returns:
  {
    "intent":     str,        # see INTENTS below
    "athlete":    str|None,   # extracted name if applicable
    "confidence": str,        # "high" | "low"
    "raw":        str,        # original message
    "parts":      list[str],  # for multi-intent messages
  }

INTENTS:
  leaderboard  → top riders this week
  service      → service due alerts
  chain        → chain replacement alerts
  attendance   → who came to events
  ghost        → inactive members
  upgrade      → upgrade candidates
  summary      → club weekly summary
  athlete      → specific athlete profile
  feedback     → owner acted on an alert (1/2/3/4 or keyword)
  clarify      → bot needs more info — reply asking owner to clarify
  help         → static help menu
  freetext     → Gemini handles this (complex / open-ended)
"""

import re
from pathlib import Path

from .feedback import is_feedback, parse_reply
from .session  import get_history

# ── Keyword maps (EN + FR) ────────────────────────
# Each intent maps to a list of trigger phrases.
# Checked as lowercase substring match.

KEYWORD_MAP: dict[str, list[str]] = {
    "leaderboard": [
        # EN
        "top 10", "top 5", "top ten", "top five", "leaderboard",
        "ranking", "rankings", "who is leading", "who's leading",
        "this week", "best riders", "weekly top",
        # FR
        "classement", "les meilleurs", "qui mène", "top du club",
        "meilleurs coureurs", "cette semaine",
    ],
    "service": [
        # EN
        "service", "maintenance", "overdue", "service due",
        "needs a service", "service alert", "bike service",
        # FR
        "entretien", "révision", "maintenance vélo", "service vélo",
        "besoin d'entretien", "entretien dû",
    ],
    "chain": [
        # EN
        "chain", "chain replacement", "chain alert", "chain due",
        # FR
        "chaîne", "chaine", "remplacement chaîne",
    ],
    "attendance": [
        # EN
        "who attended", "who came", "attendance", "who was there",
        "who joined", "last ride", "last event", "participants",
        # FR
        "qui a participé", "qui est venu", "présence", "participants",
        "dernière sortie", "dernier événement",
    ],
    "ghost": [
        # EN
        "ghost", "inactive", "missing", "not riding",
        "who hasn't", "haven't seen", "disappeared",
        # FR
        "fantôme", "inactif", "inactifs", "disparu",
        "qui ne roule pas", "absent",
    ],
    "upgrade": [
        # EN
        "upgrade", "new bike", "bike upgrade", "ready for upgrade",
        "upgrade candidate", "who should upgrade",
        # FR
        "mise à niveau", "nouveau vélo", "changer de vélo",
        "candidat mise à niveau",
    ],
    "summary": [
        # EN
        "summary", "overview", "weekly report", "how did we do",
        "club summary", "week summary", "this week's report",
        # FR
        "résumé", "bilan", "rapport hebdomadaire", "comment ça s'est passé",
    ],
    "briefing": [
        # EN
        "briefing", "friday briefing", "weekly briefing",
        "full report", "friday report", "brief me", "report",
        # FR
        "bilan complet", "rapport vendredi", "briefing",
    ],
    "help": [
        # EN
        "help", "commands", "what can you do", "menu", "options",
        # FR
        "aide", "commandes", "que peux-tu faire", "menu",
    ],
}

# ── Name extraction patterns ───────────────────────
# Matches "tell me about [Name]", "how is [Name]", etc.
NAME_TRIGGERS_EN = [
    r"(?:tell me about|how is|what about|show me|profile of|"
    r"look up|check|info on|details on|about)\s+(.+)",
    r"(.+?)(?:'s| profile| stats| service| bike| history| status)",
]
NAME_TRIGGERS_FR = [
    r"(?:dis-moi pour|comment va|montre-moi|profil de|"
    r"cherche|infos sur|détails sur|à propos de)\s+(.+)",
    r"(.+?)(?:'s| profil| stats| entretien| vélo| historique)",
]

# Known non-name words to exclude from extraction
NON_NAMES = {
    "me", "us", "the", "club", "rider", "athlete", "member",
    "le", "la", "les", "un", "une", "moi", "nous", "lui",
    "this", "that", "it", "he", "she", "they",
}


# ── Helpers ────────────────────────────────────────
def _clean(text: str) -> str:
    return text.strip().lower()

def _extract_name(text: str) -> list[str]:
    """
    Try to extract athlete name(s) from message.
    Returns list of candidate names (empty if none found).
    """
    candidates = []
    patterns   = NAME_TRIGGERS_EN + NAME_TRIGGERS_FR

    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            name = m.group(1).strip().rstrip("?.,!")
            # Filter out non-name words
            if name.lower() not in NON_NAMES and len(name) > 2:
                candidates.append(name)

    # Deduplicate preserving order
    seen, unique = set(), []
    for c in candidates:
        if c.lower() not in seen:
            seen.add(c.lower())
            unique.append(c)
    return unique


def _check_session_context(owner_id: str, text: str) -> str | None:
    """
    Use session history to resolve ambiguous messages.
    E.g. "what about his service?" after talking about João
    → returns "João Baptista" so we know who "his" refers to.
    """
    history = get_history(owner_id)
    if not history:
        return None

    # Look for athlete names in recent bot replies
    name_pattern = re.compile(
        r"\b([A-ZÁÉÍÓÚÀÂÊÎÔÛÄËÏÖÜÇ][a-záéíóúàâêîôûäëïöüç]+(?:\s+"
        r"[A-ZÁÉÍÓÚÀÂÊÎÔÛÄËÏÖÜÇ][a-záéíóúàâêîôûäëïöüç]+)*)\b"
    )
    pronouns = {"his", "her", "their", "he", "she", "they",
                "il", "elle", "son", "sa", "ses", "lui"}

    # Only resolve if message contains a pronoun
    if not any(p in text.lower().split() for p in pronouns):
        return None

    # Search last 4 messages for a name
    for msg in reversed(history[-4:]):
        matches = name_pattern.findall(msg["content"])
        for name in matches:
            if name.lower() not in NON_NAMES and len(name) > 3:
                return name

    return None


def _split_multi_intent(text: str) -> list[str]:
    """
    Split compound messages into parts.
    "service alerts and top 10" → ["service alerts", "top 10"]
    """
    splitters = [r"\band\b", r"\bet\b", r"\balso\b", r"\baussi\b", r"\+"]
    pattern   = "|".join(splitters)
    parts     = re.split(pattern, text, flags=re.IGNORECASE)
    return [p.strip() for p in parts if p.strip()]


def _classify_single(text: str) -> tuple[str, str]:
    """
    Classify a single message fragment.
    Returns (intent, confidence).
    """
    cleaned = _clean(text)

    # Check keyword map
    for intent, keywords in KEYWORD_MAP.items():
        if any(kw in cleaned for kw in keywords):
            return intent, "high"

    # Looks like athlete question but unmatched
    if any(trigger in cleaned for trigger in [
        "about", "how is", "tell me", "show", "check",
        "dis-moi", "comment va", "montre",
    ]):
        return "athlete_unknown", "low"

    return "freetext", "low"


# ── Main classifier ────────────────────────────────
def classify(message: str, owner_id: str = "") -> dict:
    """
    Classify an owner WhatsApp message.

    Parameters:
        message  : raw owner text
        owner_id : WhatsApp number — used to check session context

    Returns:
        {
          "intent":     str,
          "athlete":    str | None,
          "confidence": "high" | "low",
          "raw":        str,
          "parts":      list[str],
          "clarify_msg":str | None   # message to send back if clarify
        }
    """
    raw     = message.strip()
    cleaned = _clean(raw)

    # ── Step 1: Feedback? ──────────────────────────
    if is_feedback(cleaned):
        return {
            "intent":      "feedback",
            "action":      parse_reply(cleaned),
            "athlete":     None,
            "confidence":  "high",
            "raw":         raw,
            "parts":       [raw],
            "clarify_msg": None,
        }

    # ── Step 2: Multi-intent split ─────────────────
    parts = _split_multi_intent(raw)
    if len(parts) > 1:
        sub_intents = [_classify_single(p)[0] for p in parts]
        # If all parts are known → handle as list
        if "freetext" not in sub_intents and "athlete_unknown" not in sub_intents:
            return {
                "intent":      "multi",
                "parts":       parts,
                "sub_intents": sub_intents,
                "athlete":     None,
                "confidence":  "high",
                "raw":         raw,
                "clarify_msg": None,
            }

    # ── Step 3: Known command? ─────────────────────
    intent, confidence = _classify_single(cleaned)

    if intent not in ("freetext", "athlete_unknown"):
        return {
            "intent":      intent,
            "athlete":     None,
            "confidence":  confidence,
            "raw":         raw,
            "parts":       parts,
            "clarify_msg": None,
        }

    # ── Step 4: Athlete lookup? ────────────────────
    names = _extract_name(raw)

    # Try session context for pronouns ("his", "her")
    if not names:
        ctx_name = _check_session_context(owner_id, raw)
        if ctx_name:
            names = [ctx_name]

    if len(names) == 1:
        return {
            "intent":      "athlete",
            "athlete":     names[0],
            "confidence":  "high",
            "raw":         raw,
            "parts":       parts,
            "clarify_msg": None,
        }

    if len(names) > 1:
        # Multiple names — ask what to compare
        return {
            "intent":      "clarify",
            "athlete":     None,
            "confidence":  "low",
            "raw":         raw,
            "parts":       parts,
            "clarify_msg": (
                f"I found multiple athletes: {', '.join(names)}.\n"
                f"What would you like to know?\n"
                f"1 · Compare their mileage\n"
                f"2 · Service status for both\n"
                f"3 · Just tell me about {names[0]}"
            ),
        }

    # Looks like athlete question but no name found
    if intent == "athlete_unknown":
        return {
            "intent":      "clarify",
            "athlete":     None,
            "confidence":  "low",
            "raw":         raw,
            "parts":       parts,
            "clarify_msg": (
                "Which athlete did you mean?\n"
                "Please reply with their name and I'll look them up."
            ),
        }

    # ── Step 5: Free-text → Gemini ─────────────────
    return {
        "intent":      "freetext",
        "athlete":     None,
        "confidence":  "low",
        "raw":         raw,
        "parts":       parts,
        "clarify_msg": None,
    }


# ── Static help menu ───────────────────────────────
HELP_TEXT = """ClubRide.Ai — available commands:

📋 *briefing* — full weekly report (upgrades + service + ghosts)
📊 *top 10* — this week's leaderboard
🔧 *service* — athletes due for service
🔗 *chain* — chain replacement alerts
📅 *attendance* — recent event attendance
👻 *ghost* — inactive members
⭐ *upgrade* — upgrade candidates
📋 *summary* — weekly club overview
👤 *tell me about [name]* — athlete profile

After an alert, reply:
1 · Done   2 · Contacted   3 · Snooze   4 · Ignore"""


# ── CLI test ───────────────────────────────────────
if __name__ == "__main__":
    tests = [
        ("2",                           "feedback → contacted"),
        ("done",                        "feedback → done"),
        ("top 10",                      "leaderboard EN"),
        ("classement",                  "leaderboard FR"),
        ("who needs a service?",        "service EN"),
        ("entretien",                   "service FR"),
        ("tell me about João",          "athlete lookup"),
        ("how is the guy who wins?",    "clarify — no name"),
        ("João and Alex who rides more","clarify — two names"),
        ("service and top 10",          "multi-intent"),
        ("should I call João this week?","freetext → Gemini"),
    ]

    print("── Intent classifier test ────────────────────\n")
    for msg, expected in tests:
        result = classify(msg, owner_id="test")
        clarify = f" → '{result['clarify_msg'][:40]}...'" if result.get("clarify_msg") else ""
        athlete = f" [{result['athlete']}]" if result.get("athlete") else ""
        print(f"  '{msg[:35]:35}'"
              f"  intent={result['intent']:12}"
              f"  conf={result['confidence']:4}"
              f"{athlete}{clarify}")
        print(f"   expected: {expected}\n")

    print("✅ intent_classifier.py — zero Gemini in all deterministic cases")
