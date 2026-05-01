# Triage Log

Running log of issues found, fixed, and outstanding. Updated as the product evolves.

---

## Fixed — MVP session (2026-05-01)

| # | Issue | Impact | Fix |
|---|---|---|---|
| 1 | Off-topic questions routed to `briefing` instead of command menu | Owner received irrelevant briefing for unrelated messages | Added `show_help` tool + explicit 10-rule routing system in Gemini prompt |
| 2 | Keyword fallback too broad — `"list of"`, `"members"`, `"resume"` matched inside unrelated sentences | Any message containing those words triggered briefing | Removed overly broad keywords from fallback map |
| 3 | Draft message picked service signal over upgrade for riders flagged for both | Aleksei (upgrade candidate + chain due) received a service draft instead of upgrade draft | Fixed signal priority: upgrade → service → chain → at-risk → engagement |
| 4 | At-risk signal missing from draft message generator | Inactive members fell through to generic "general engagement" draft | Added ghost signal with weeks-absent context and coffee-invite CTA |
| 5 | Recruit list included riders with no confirmed location | Non-local or anonymous riders appeared as invite targets | Changed location filter: skip if missing or non-Lausanne/Vaud (was: skip only if non-local) |
| 6 | Future event date labelled "last seen" instead of "upcoming" | Giovanni's May 2 registration showed as "last 2026-05-02" on May 1 | Added `date.today()` comparison — labels future dates as "upcoming" |
| 7 | Draft messages had no call to action | Message complimented the rider but gave no next step | Updated `_DRAFT_SYSTEM` prompt: sentence 1 = data reference, sentence 2 = explicit CTA per signal type |
| 8 | Personal athlete name hardcoded in `retriever.py` | Name would appear in public GitHub repo | Moved to `EXCLUDED_ATHLETES` env var in `.env` (gitignored) |

---

## Open Issues

| # | Issue | Severity | Notes |
|---|---|---|---|
| 1 | Scheduler not wired | High | Friday briefing, data scraping, and Monday update require manual trigger; APScheduler config exists in `config.json` but not connected |
| 2 | "Full list of members" queries occasionally still route to briefing | Medium | Gemini semantically maps member-list requests to the closest tool; deterministic gate (validate tool call against trigger words) was scoped out of MVP |
| 3 | At-risk detection inconsistency in draft message | Low | `_handle_draft_message` calculates `weeks_absent` from `last_seen` in athlete profile; `get_at_risk_members` tool uses its own calculation from attendance history — may differ by 1-2 weeks |
| 4 | Hidden gear — ~10-20% of athletes | Structural | Athletes with private Strava gear settings have no bike data regardless of follow status; no upgrade or service signal possible for these riders |
| 5 | Follower gap | Structural | Athletes who attend rides but don't follow the owner appear as attendance-only; no stats, no bike, no scoring. Mitigation: owner proactively follows all regular attendees |

---

## Technical Debt

| # | Item | Notes |
|---|---|---|
| 1 | Keyword fallback + Gemini routing overlap | Two routing layers with partially duplicated logic; should be unified into a single deterministic pre-check + Gemini for name extraction only |
| 2 | Session history in flat JSON | `data/session.json` works for single owner; will not scale to multi-club or multi-owner without a proper store |
| 3 | No retry on Playwright scraping failures | If a scrape fails mid-run, partial data is written silently; needs atomic write + failure alerting |
| 4 | Synthetic CRM data | Bike purchase history, service dates, and purchase source are inferred from Strava profiles; accuracy is estimated, not verified. Real CRM integration is Phase 3 |
