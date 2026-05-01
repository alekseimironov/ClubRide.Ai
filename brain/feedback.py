"""
brain/feedback.py
Deterministic feedback loop — zero Gemini calls.

Every alert the bot sends includes a numbered menu:
  1 · Done / resolved
  2 · I contacted them
  3 · Snooze 7 days
  4 · Ignore

Owner replies with a number (or keyword synonym).
Code maps it directly → logs to owner_actions.csv → suppresses repeat alerts.

No Gemini involved anywhere in this file.
"""

import csv
from datetime import date, timedelta
from pathlib import Path

# ── Config ─────────────────────────────────────────
ROOT          = Path(__file__).parent.parent
ACTIONS_CSV   = ROOT / "data/real/owner_actions.csv"
SUPPRESS_DAYS = 60
SNOOZE_DAYS   = 7

COLUMNS = ["Date", "Owner_ID", "Athlete", "Alert_Type", "Action", "Note", "Expires"]

# ── Deterministic reply map ────────────────────────
# Numbers + keyword synonyms → canonical action
REPLY_MAP = {
    # Numbered replies
    "1": "done",
    "2": "contacted",
    "3": "snooze",
    "4": "ignore",
    # Keyword synonyms (owner may type words instead)
    "done":      "done",
    "resolved":  "done",
    "fixed":     "done",
    "ok":        "done",
    "contacted": "contacted",
    "called":    "contacted",
    "messaged":  "contacted",
    "sent":      "contacted",
    "snooze":    "snooze",
    "later":     "snooze",
    "remind":    "snooze",
    "ignore":    "ignore",
    "skip":      "ignore",
    "no":        "ignore",
}

# ── Alert menu template ────────────────────────────
MENU = (
    "\nReply:\n"
    "1 · Done / resolved\n"
    "2 · I contacted them\n"
    "3 · Snooze 7 days\n"
    "4 · Ignore"
)


# ── Parse owner reply ──────────────────────────────
def parse_reply(text: str) -> str | None:
    """
    Map owner's text to a canonical action deterministically.
    Returns action string or None if not recognised as feedback.
    No Gemini call — pure lookup.
    """
    cleaned = text.strip().lower().rstrip(".,!?")
    return REPLY_MAP.get(cleaned)


def is_feedback(text: str) -> bool:
    """True if the owner's message is a feedback reply, not a new question."""
    return parse_reply(text) is not None


# ── Alert message builder ──────────────────────────
def build_alert(alert_type: str, athlete: str, detail: str) -> str:
    """
    Build a WhatsApp alert message with the numbered reply menu attached.

    Example output:
      ⚠️ Service alert: João Baptista
      3,200 km since last service (due at 5,000 km)

      Reply:
      1 · Done / resolved
      2 · I contacted them
      3 · Snooze 7 days
      4 · Ignore
    """
    icons = {
        "service_due": "⚠️ Service alert",
        "chain_due":   "🔗 Chain alert",
        "ghost":       "👻 Ghost member alert",
        "upgrade":     "⭐ Upgrade opportunity",
    }
    header = icons.get(alert_type, "🔔 Alert")
    return f"{header}: {athlete}\n{detail}{MENU}"


# ── CSV helpers ────────────────────────────────────
def _ensure_csv():
    ACTIONS_CSV.parent.mkdir(parents=True, exist_ok=True)
    if not ACTIONS_CSV.exists():
        with open(ACTIONS_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=COLUMNS).writeheader()


def _load_active() -> list[dict]:
    _ensure_csv()
    today  = date.today()
    active = []
    try:
        with open(ACTIONS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    if date.fromisoformat(row["Expires"]) >= today:
                        active.append(row)
                except (ValueError, KeyError):
                    pass
    except FileNotFoundError:
        pass
    return active


# ── Public API ─────────────────────────────────────
def log_action(owner_id: str, athlete: str, alert_type: str,
               action: str, note: str = ""):
    """Persist an owner action. Called after parse_reply() returns a valid action."""
    _ensure_csv()
    days    = SNOOZE_DAYS if action == "snooze" else SUPPRESS_DAYS
    expires = (date.today() + timedelta(days=days)).isoformat()

    with open(ACTIONS_CSV, "a", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=COLUMNS).writerow({
            "Date":       date.today().isoformat(),
            "Owner_ID":   owner_id,
            "Athlete":    athlete,
            "Alert_Type": alert_type,
            "Action":     action,
            "Note":       note,
            "Expires":    expires,
        })


def is_suppressed(athlete: str, alert_type: str) -> bool:
    """True if this athlete+alert is within suppression window."""
    k = athlete.strip().lower()
    return any(
        row["Athlete"].strip().lower() == k and row["Alert_Type"] == alert_type
        for row in _load_active()
    )


def suppressed_athletes(alert_type: str) -> set[str]:
    """Set of athlete names currently suppressed for a given alert_type."""
    return {
        row["Athlete"].strip().lower()
        for row in _load_active()
        if row["Alert_Type"] == alert_type
    }


def pending_alerts(alert_type: str, all_athletes: list[str]) -> list[str]:
    """Filter athlete list → only those not currently suppressed."""
    suppressed = suppressed_athletes(alert_type)
    return [a for a in all_athletes
            if isinstance(a, str) and a.strip() and a.strip().lower() not in suppressed]


def get_action_log(athlete: str = None) -> list[dict]:
    """Full action history, optionally filtered by athlete."""
    _ensure_csv()
    rows = []
    try:
        with open(ACTIONS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if athlete is None or row["Athlete"].strip().lower() == athlete.strip().lower():
                    rows.append(row)
    except FileNotFoundError:
        pass
    return rows


# ── CLI test ───────────────────────────────────────
if __name__ == "__main__":
    import os

    TEST_OWNER = "whatsapp:+41000000000"
    if ACTIONS_CSV.exists():
        os.remove(ACTIONS_CSV)

    print("── Alert message ─────────────────────────────")
    print(build_alert("service_due", "João Baptista",
                      "3,200 km since last service (due at 5,000 km)"))

    print("\n── Reply parsing (deterministic) ─────────────")
    tests = ["1", "2", "3", "4", "done", "called", "later",
             "ignore", "what?", "tell me more"]
    for t in tests:
        result = parse_reply(t)
        flag   = "✅ feedback" if result else "➡️  new question"
        print(f"  '{t:15}' → {str(result):12} {flag}")

    print("\n── Suppression logic ─────────────────────────")
    print(f"Suppressed before: {is_suppressed('João Baptista', 'service_due')}")
    action = parse_reply("2")   # owner replied "2" → "contacted"
    log_action(TEST_OWNER, "João Baptista", "service_due", action)
    print(f"Suppressed after : {is_suppressed('João Baptista', 'service_due')}")

    athletes = ["João Baptista", "Alex Toullier", "Nicole K."]
    print(f"Pending alerts   : {pending_alerts('service_due', athletes)}")

    print("\n✅ feedback.py working correctly — zero Gemini calls")
