# ClubRide.Ai

WhatsApp intelligence bot for cycling club owners. Turns Strava activity and event attendance data into actionable signals — upgrade candidates, service alerts, at-risk members — delivered as a conversation.

---

## What it does

Send a message to your WhatsApp number and get an instant answer:

| Command | Response |
|---|---|
| `top 10` | This week's community leaderboard |
| `upgrade` | Members riding seriously on a lower-tier bike |
| `service` | Bikes overdue for service or chain replacement |
| `at risk` | Regulars who stopped showing up (6+ weeks absent) |
| `recruit` | Serious local solo riders worth inviting |
| `who to talk to` | Top 1 upgrade + top 1 service contact for this weekend |
| `briefing` | Full weekly report |
| `tell me about [name]` | Full athlete profile |
| `draft for [name]` | Ready-to-forward WhatsApp message, tailored to their signal |

You can also add custom instructions inline:
> *"draft for Tomasz, mention we just got new Trek Émonda in stock"*

---

## Architecture

```
WhatsApp → Twilio → POST /whatsapp → brain/prompter.py → Gemini (tool routing)
                                                        → retriever.py (CSV data)
                                                        → reply via Twilio
```

- **Flask** — webhook server
- **Twilio** — WhatsApp in/out
- **Gemini 2.5 Flash** — intent routing + draft message generation
- **Pandas** — CSV data layer (Strava leaderboard, attendance, bike profiles)
- **Playwright** — Strava scraping (leaderboard + events)

---

## Setup

### 1. Clone and install

```bash
git clone https://github.com/alekseimironov/ClubRide.Ai.git
cd ClubRide.Ai
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure environment

```bash
cp .env.example .env
```

Fill in `.env`:

```
GEMINI_API_KEY=...
TWILIO_ACCOUNT_SID=...
TWILIO_AUTH_TOKEN=...
TWILIO_WHATSAPP_FROM=whatsapp:+14155238886
OWNER_WHATSAPP=whatsapp:+41XXXXXXXXX
STRAVA_CLIENT_ID=...
STRAVA_CLIENT_SECRET=...
FLASK_SECRET_KEY=...
EXCLUDED_ATHLETES=full name to hide,another name
```

### 3. Add your data

Place CSV files in `data/real/` — the bot expects:

| File | Contents |
|---|---|
| `leaderboard_history.csv` | Weekly km per athlete (scraped from Strava) |
| `historical_attendance_TNCE.csv` | Event attendance records |
| `athlete_profiles.csv` | Rider tier, bike, speed stats |
| `athlete_bikes.csv` | Bike model, brand, purchase data |

Run the scrapers in `scrapers/` to populate these files.

### 4. Run locally

```bash
python app.py
```

Expose with ngrok:

```bash
ngrok http 5000
```

Copy the `https://` URL → paste into **Twilio sandbox webhook** field as:
```
https://xxxx.ngrok.io/whatsapp
```

Send any message to your Twilio WhatsApp sandbox number to test.

---

## Data privacy

- All personal data stays in `data/` — excluded from git by `.gitignore`
- API keys and credentials stay in `.env` — never committed
- Athlete names to exclude from all bot tools go in `EXCLUDED_ATHLETES` in `.env`
- Bot only processes data for the club owner's own Strava club

---

## Deployment (Render)

1. Push to GitHub
2. Create a new **Web Service** on [render.com](https://render.com)
3. Connect your GitHub repo
4. Set build command: `pip install -r requirements.txt && playwright install chromium`
5. Set start command: `python app.py`
6. Add all `.env` variables in Render's environment settings
7. Point Twilio webhook at your Render URL: `https://your-app.onrender.com/whatsapp`

---

## Project structure

```
ClubRide.Ai/
├── app.py                  # Flask entry point
├── config.json             # Club settings (name, service intervals, scheduler)
├── bot/
│   ├── webhook.py          # Incoming WhatsApp handler
│   └── whatsapp_sender.py  # Outgoing message delivery
├── brain/
│   ├── prompter.py         # Gemini routing + formatters + draft messages
│   ├── retriever.py        # CSV data layer with caching
│   ├── scorer.py           # Upgrade + service scoring
│   ├── feedback.py         # Alert reply handling (1-4 replies)
│   └── session.py          # Conversation memory
├── scrapers/               # Strava leaderboard + event scrapers
├── data/
│   ├── real/               # Production CSVs (gitignored)
│   └── synthetic/          # Synthetic data for testing (gitignored)
└── tests/
```

---

## Status

MVP — Phase 1. Scheduler (auto Friday briefing) and Render deployment are Phase 2.
