"""
bot/whatsapp_sender.py
Sends WhatsApp messages via Twilio API.
Single responsibility: take a string, deliver it to a WhatsApp number.
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from twilio.rest import Client

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

_client = None


def _get_client() -> Client:
    """Lazy-init Twilio client — only created on first send."""
    global _client
    if _client is None:
        _client = Client(
            os.getenv("TWILIO_ACCOUNT_SID"),
            os.getenv("TWILIO_AUTH_TOKEN"),
        )
    return _client


def send(to: str, body: str) -> str:
    """
    Send a WhatsApp message.

    Parameters:
        to   : recipient in 'whatsapp:+41XXXXXXXXX' format
        body : plain text message (max ~1,600 chars before WhatsApp truncates)

    Returns:
        Twilio message SID on success, error string on failure.
    """
    # WhatsApp has a practical limit — split if needed
    if len(body) > 1500:
        body = body[:1497] + "..."

    try:
        msg = _get_client().messages.create(
            from_=os.getenv("TWILIO_WHATSAPP_FROM"),
            to=to,
            body=body,
        )
        print(f"  ✅ Sent to {to}  SID={msg.sid}")
        return msg.sid
    except Exception as e:
        print(f"  ❌ Send failed: {e}")
        return f"error: {e}"


def send_to_owner(body: str) -> str:
    """Shortcut — sends to the configured owner number."""
    owner = os.getenv("OWNER_WHATSAPP")
    if not owner:
        print("  ❌ OWNER_WHATSAPP not set in .env")
        return "error: no owner number"
    return send(owner, body)


# ── CLI test ───────────────────────────────────────
if __name__ == "__main__":
    print("Sending test message to owner...")
    sid = send_to_owner("ClubRide.Ai is alive. Bot is connected.")
    print(f"Result: {sid}")
