"""
brain/session.py
Manages conversation memory across WhatsApp messages.

Stores the last MAX_TURNS owner/bot message pairs in data/session.json.
Every Gemini call receives this history so follow-up questions work:

  Owner: "tell me about João"
  Bot:   "João rode 580km this week, ranked #1..."
  Owner: "what about his service?"   ← brain knows who "his" refers to
  Bot:   "João is at 3,200km since last service..."

Session is per-owner (keyed by WhatsApp number).
Resets after IDLE_HOURS of inactivity.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

# ── Config ─────────────────────────────────────────
ROOT       = Path(__file__).parent.parent
SESSION_FILE = ROOT / "data" / "session.json"
MAX_TURNS    = 5        # number of owner+bot pairs to keep
IDLE_HOURS   = 4        # reset session after 4h of inactivity


# ── Load / Save ────────────────────────────────────
def _load() -> dict:
    try:
        with open(SESSION_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save(data: dict):
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(SESSION_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ── Public API ─────────────────────────────────────
def get_history(owner_id: str) -> list[dict]:
    """
    Return conversation history for this owner as a list of dicts:
    [{"role": "user"|"assistant", "content": "...text..."}, ...]

    Returns [] if session is new or expired.
    """
    data  = _load()
    entry = data.get(owner_id)
    if not entry:
        return []

    # Reset if idle too long
    last_active = datetime.fromisoformat(entry.get("last_active", "2000-01-01"))
    if datetime.now() - last_active > timedelta(hours=IDLE_HOURS):
        clear(owner_id)
        return []

    return entry.get("history", [])


def add_turn(owner_id: str, user_message: str, bot_reply: str):
    """
    Append one owner/bot exchange to the session.
    Trims to MAX_TURNS pairs automatically.
    """
    data    = _load()
    entry   = data.get(owner_id, {"history": []})
    history = entry.get("history", [])

    history.append({"role": "user",      "content": user_message})
    history.append({"role": "assistant", "content": bot_reply})

    # Keep only the last MAX_TURNS × 2 messages
    history = history[-(MAX_TURNS * 2):]

    data[owner_id] = {
        "history":     history,
        "last_active": datetime.now().isoformat(),
    }
    _save(data)


def clear(owner_id: str):
    """Reset session for this owner (e.g. after long idle or explicit reset)."""
    data = _load()
    if owner_id in data:
        del data[owner_id]
        _save(data)


def format_for_prompt(history: list[dict]) -> str:
    """
    Format history as a readable block for injection into a Gemini prompt.
    Returns empty string if no history.
    """
    if not history:
        return ""
    lines = []
    for msg in history:
        role = "Owner" if msg["role"] == "user" else "Bot"
        lines.append(f"{role}: {msg['content']}")
    return "PREVIOUS CONVERSATION:\n" + "\n".join(lines)


# ── CLI test ───────────────────────────────────────
if __name__ == "__main__":
    TEST_OWNER = "whatsapp:+41000000000"

    print("Testing session.py...\n")

    clear(TEST_OWNER)
    print("History after clear:", get_history(TEST_OWNER))

    add_turn(TEST_OWNER, "Tell me about João", "João rode 580km this week, ranked #1.")
    add_turn(TEST_OWNER, "What about his service?", "João is at 3,200km since last service.")
    add_turn(TEST_OWNER, "Should I reach out?", "Yes, consider a check-in this week.")

    history = get_history(TEST_OWNER)
    print(f"History ({len(history)} messages):")
    print(format_for_prompt(history))

    # Test MAX_TURNS trim — add 10 more turns
    for i in range(10):
        add_turn(TEST_OWNER, f"Question {i}", f"Answer {i}")
    history = get_history(TEST_OWNER)
    print(f"\nAfter 13 total turns, history kept: {len(history)} messages "
          f"(max {MAX_TURNS * 2})")

    clear(TEST_OWNER)
    print("\n✅ session.py working correctly")
