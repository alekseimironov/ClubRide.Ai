"""
bot/webhook.py
Flask blueprint — receives incoming WhatsApp messages from Twilio.

Twilio POST fields:
  From : sender's WhatsApp number  e.g. 'whatsapp:+41XXXXXXXXX'
  Body : message text

Flow:
  1. Parse From + Body
  2. Validate sender is the owner (security)
  3. Pass to prompter.handle()
  4. Send reply via whatsapp_sender
  5. Return 200 to Twilio immediately
"""

import os
import xml.sax.saxutils as saxutils
from pathlib import Path

from dotenv import load_dotenv
from flask import Blueprint, request, make_response

from brain.prompter     import handle
from bot.whatsapp_sender import send

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

webhook_bp = Blueprint("webhook", __name__)

CLUB_ID = 318940


def _is_owner(from_number: str) -> bool:
    """
    Only the configured owner can interact with the bot.
    Prevents strangers from querying club data if the number leaks.
    """
    owner = os.getenv("OWNER_WHATSAPP", "").strip()
    return from_number.strip() == owner


@webhook_bp.route("/whatsapp", methods=["POST"])
def whatsapp_webhook():
    """
    Main entry point for all incoming WhatsApp messages.
    Twilio expects a 200 response — we process and reply asynchronously.
    """
    from_number = request.form.get("From", "").strip()
    body        = request.form.get("Body", "").strip()

    print(f"\n📨 Message from {from_number}: '{body}'")

    # ── Security: owner-only ───────────────────────
    if not _is_owner(from_number):
        print(f"  ⛔ Rejected — unknown sender: {from_number}")
        return jsonify({"status": "rejected"}), 200

    if not body:
        return _twiml("I received an empty message. Type *help* for commands.")

    # ── Process + reply via TwiML (zero Twilio message credits consumed) ──
    try:
        reply = handle(body, owner_id=from_number, club_id=CLUB_ID)
        print(f"  ✅ Reply sent ({len(reply)} chars)")
    except Exception as e:
        reply = "Sorry, something went wrong. Please try again."
        print(f"  ❌ Error: {e}")

    return _twiml(reply)


def _twiml(message: str):
    """Return a TwiML response — does not consume Twilio outbound message credits."""
    safe = saxutils.escape(message)
    xml  = f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{safe}</Message></Response>'
    resp = make_response(xml)
    resp.headers["Content-Type"] = "text/xml"
    return resp


@webhook_bp.route("/health", methods=["GET"])
def health():
    """Health check endpoint — confirms bot is running."""
    return jsonify({
        "status":  "online",
        "club_id": CLUB_ID,
        "bot":     "ClubRide.Ai",
    }), 200
