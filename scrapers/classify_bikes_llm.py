"""
scrapers/classify_bikes_llm.py
Uses Gemini Flash to classify each unique bike as:
  category: road | gravel | mtb | indoor | unknown
  tier:     entry | mid | top | null  (null for non-road)

Reads:  data/real/athlete_bikes.csv      (339 unique bikes)
Cache:  data/synthetic/bike_classifications.json  (skip already done)
Output: data/synthetic/bike_classifications.json  (appended)

Run once — re-run is safe, skips already-classified bikes.
Run: python scrapers/classify_bikes_llm.py
"""

import json
import os
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google import genai
from google.genai import types

ROOT       = Path(__file__).parent.parent
BIKES_CSV  = ROOT / "data/real/athlete_bikes.csv"
CACHE_FILE = ROOT / "data/synthetic/bike_classifications.json"
BATCH_SIZE = 30   # bikes per Gemini call

load_dotenv(ROOT / ".env")

SYSTEM_PROMPT = """You are a cycling expert. Classify each bike entry.

For each bike return:
  category: "road" | "gravel" | "mtb" | "indoor" | "unknown"
  tier:     "entry" | "mid" | "top"

Tier is ALWAYS required — assign based on brand/model quality regardless of category.
A top-tier gravel bike is still "top". A cheap MTB is still "entry".

Tier definitions (based on equivalent market value and brand position):
  top   — pro-grade, flagship model, typically >€3000 new
          (Pinarello Dogma, S-Works, Canyon Aeroad CF SLX, OPEN WI.DE/MIN.D,
           Colnago V3RS, Cervélo R5, Look 795, BMC SLR01, Specialized Aethos…)
  mid   — quality carbon or high-end alloy, €1200–3000
          (Canyon Endurace CF, Tarmac SL7, Trek Emonda SL, BMC Roadmachine,
           Scott Addict, Giant TCR Advanced, Orbea Orca, Bianchi Infinito…)
  entry — alloy or budget carbon, <€1200
          (Specialized Allez E5, Trek Domane AL, Decathlon Van Rysel, CAAD13,
           basic MTB, entry gravel…)

Category rules:
- Trainers, Zwift, Tacx, Wahoo Kickr → category=indoor
- MTB, VTT, Scalpel, Stumpjumper, Genius, Scale → category=mtb
- Gravel bikes (Grizl, Grail, Aspero, Topstone, Inflite, gravel in name) → category=gravel
- Everything else → category=road (default) or category=unknown if unrecognisable nickname

Brand-only entries (no model name): use brand reputation.
  Canyon → road, mid. OPEN → road, top. Pinarello → road, top.
  Bianchi → road, mid. Scott → road, mid. BMC → road, mid.
  Centurion → road, entry. Decathlon/Btwin → road, entry.

Return a JSON array, one object per bike:
[
  {"brand": "Bianchi", "name": "Bianchi gravel", "category": "gravel", "tier": "mid"},
  ...
]
Only return the JSON array, no markdown, no explanation.
"""


def load_cache() -> dict:
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        return {_cache_key(e["brand"], e["name"]): e for e in data}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_cache(cache: dict):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    records = list(cache.values())
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)


def _cache_key(brand: str, name: str) -> str:
    return f"{brand.strip().lower()}|{name.strip().lower()}"


def classify_batch(client, bikes: list[dict]) -> list[dict]:
    """Send one batch to Gemini Flash, return classified list."""
    bike_list = "\n".join(
        f'{i+1}. Brand: "{b["brand"]}" | Name: "{b["name"]}"'
        for i, b in enumerate(bikes)
    )
    prompt = f"Classify these {len(bikes)} bikes:\n\n{bike_list}"

    response = client.models.generate_content(
        model="gemini-2.5-flash-lite",
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            response_mime_type="application/json",
            temperature=0.1,
        ),
    )

    raw = response.text.strip()
    # Strip markdown fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    return json.loads(raw)


def run():
    print(f"\n{'='*60}")
    print(f"  LLM BIKE CLASSIFIER  (Gemini Flash)")
    print(f"{'='*60}\n")

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: GEMINI_API_KEY not found in .env")
        return
    client = genai.Client(api_key=api_key)

    bikes_df = pd.read_csv(BIKES_CSV, dtype=str)
    unique   = (bikes_df[["Brand", "Bike_Name"]]
                .drop_duplicates()
                .dropna(subset=["Bike_Name"])
                .rename(columns={"Brand": "brand", "Bike_Name": "name"}))

    cache   = load_cache()
    pending = [
        row for _, row in unique.iterrows()
        if _cache_key(str(row["brand"]), str(row["name"])) not in cache
    ]

    print(f"  Total unique bikes   : {len(unique)}")
    print(f"  Already classified   : {len(cache)}")
    print(f"  To classify now      : {len(pending)}\n")

    if not pending:
        print("  All bikes already classified — cache is up to date.")
        _print_summary(cache)
        return

    total_batches = (len(pending) + BATCH_SIZE - 1) // BATCH_SIZE
    classified = 0
    errors     = 0

    for i in range(0, len(pending), BATCH_SIZE):
        batch    = pending[i : i + BATCH_SIZE]
        batch_n  = i // BATCH_SIZE + 1
        print(f"  Batch {batch_n}/{total_batches}  ({len(batch)} bikes)...", end=" ")

        try:
            results = classify_batch(client, [{"brand": str(r["brand"]), "name": str(r["name"])} for r in batch])

            for res in results:
                key = _cache_key(res.get("brand",""), res.get("name",""))
                cache[key] = {
                    "brand":    res.get("brand", ""),
                    "name":     res.get("name", ""),
                    "category": res.get("category", "unknown"),
                    "tier":     res.get("tier"),
                }
            classified += len(results)
            save_cache(cache)
            print(f"done  ({classified} total classified)")

        except Exception as e:
            errors += 1
            print(f"ERROR: {e}")

        if i + BATCH_SIZE < len(pending):
            time.sleep(1.5)   # avoid rate limit

    print(f"\n  Classified : {classified}")
    print(f"  Errors     : {errors}")
    print(f"  Cache      -> {CACHE_FILE}\n")
    _print_summary(cache)
    print(f"\n  Run next: python scrapers/build_bike_model.py")
    print(f"{'='*60}\n")


def _print_summary(cache: dict):
    from collections import Counter
    cats = Counter(e["category"] for e in cache.values())
    tiers = Counter(e["tier"] for e in cache.values() if e.get("tier"))
    print(f"  Category breakdown:")
    for cat, n in sorted(cats.items()):
        print(f"    {cat:<12} {n}")
    print(f"  Road tier breakdown:")
    for tier in ["entry", "mid", "top"]:
        print(f"    {tier:<12} {tiers.get(tier, 0)}")


if __name__ == "__main__":
    run()
