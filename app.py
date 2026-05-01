"""
app.py
ClubRide.Ai — Flask entry point.

Starts the web server and registers the WhatsApp webhook.
Run: python app.py

Endpoints:
  POST /whatsapp  ← Twilio sends incoming messages here
  GET  /health    ← confirms bot is running
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask

from bot.webhook import webhook_bp

ROOT = Path(__file__).parent
load_dotenv(ROOT / ".env")

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-key")

# Register webhook routes
app.register_blueprint(webhook_bp)


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    debug = os.getenv("FLASK_ENV") == "development"

    print(f"""
╔══════════════════════════════════════════╗
║         ClubRide.Ai is starting          ║
╠══════════════════════════════════════════╣
║  Webhook : POST /whatsapp                ║
║  Health  : GET  /health                  ║
║  Port    : {port:<31} ║
║  Debug   : {str(debug):<31} ║
╚══════════════════════════════════════════╝

Next step: expose with ngrok
  ngrok http {port}
  → copy the https URL
  → paste into Twilio sandbox webhook field
""")

    app.run(host="0.0.0.0", port=port, debug=debug)
