"""
brain/strings.py
EN / FR translation dictionary for all user-facing output.

Usage:
    from .strings import t
    t("key", lang)           # plain lookup, falls back to EN
    t("key", lang, n=5)      # with template substitution
"""

S: dict[str, dict[str, str]] = {

    # ══════════════════════════════════════════════════════════════════════════
    "en": {

        # ── Language toggle ──────────────────────────────────────────────────
        "lang_set_en": "Switched to English. 🇬🇧",
        "lang_set_fr": "Switched to French. 🇫🇷",

        # ── Help ─────────────────────────────────────────────────────────────
        "help": (
            "*TNCE | ClubRide.Ai* — your club assistant\n"
            "\n"
            "🏆 *top 10* — this week's leaderboard\n"
            "🎯 *who to talk to* — 2 weekend priorities\n"
            "🚲 *upgrade* — riders ready for a new bike\n"
            "🔧 *service* — bikes overdue for service or chain\n"
            "⚠️ *at risk* — loyal members going quiet\n"
            "👥 *recruit* — solo riders worth inviting\n"
            "🏅 *loyal* — most active community members\n"
            "📋 *briefing* — full weekly report\n"
            "\n"
            "*Rider profile:*\n"
            "👤 \"tell me about Marko\"\n"
            "👤 \"who is Julien\"\n"
            "\n"
            "*Personalised messages:*\n"
            "✉️ \"draft for [name]\" — auto-detects signal (upgrade / service / re-engage)\n"
            "✉️ \"draft for [name], mention [your note]\" — add your own angle\n"
            "\n"
            "*Language:*\n"
            "🇬🇧 type in English → replies in English\n"
            "🇫🇷 écris en français → réponses en français\n"
        ),

        # ── Leaderboard ──────────────────────────────────────────────────────
        "lb_no_data":      "No leaderboard data available yet.",
        "lb_no_community": "No community members on the W{week}/{year} leaderboard yet.",
        "lb_came":         "came to the club ride",
        "lb_registered":   "registered for",

        # ── Service ──────────────────────────────────────────────────────────
        "svc_no_alerts":    "No service or chain alerts pending — all clear.",
        "svc_due_header":   "Service Due ({n}) *[estimated data]*",
        "chain_due_header": "\nChain Due ({n}) *[estimated data]*",
        "svc_since":        "km since service",
        "chain_since":      "km since chain",

        # ── Upgrade ──────────────────────────────────────────────────────────
        "upg_none":        "No upgrade candidates right now.",
        "upg_header":      "Upgrade Candidates ({n})",
        "upg_km_wk":       "{wk:.0f} km/wk",
        "upg_km_wk_est":   "~{wk:.0f} km/wk (est.)",
        "upg_bike_unk":    "bike unknown",
        "upg_km_on_bike":  "{km:,.0f}km on this bike",
        "upg_bought_tnce": "bought TNCE {yr} (est.)",
        "upg_bought_ext":  "bought External (est.)",
        "upg_solo":        "solo rider",
        "upg_occasional":  "occasional ({n} events)",
        "upg_regular":     "regular ({n} events)",
        "upg_loyal":       "loyal ({n} events)",

        # ── Weekend priorities ───────────────────────────────────────────────
        "wp_none":   "No priorities this weekend — all clear.",
        "wp_header": "*ACT THIS WEEKEND*",

        # ── Recruits ─────────────────────────────────────────────────────────
        "rec_none":    "No solo riders to recruit right now.",
        "rec_header":  "Potential recruits ({n}) — serious riders who never joined a ride",
        "rec_footer":  "\nThese riders are active but ride solo — worth a personal invitation.",
        "rec_no_bike": "no bike data",
        "rec_km_wk":   "{km:.0f} km/wk",
        "rec_tier":    "{tier} tier",
        "rec_rider":   "{tier} rider",

        # ── At risk ──────────────────────────────────────────────────────────
        "atrisk_none":    "👥 All regulars are still showing up — no one at risk.",
        "atrisk_header":  "👥 *Loyal riders going quiet ({n})*",
        "atrisk_sub":     "_These members rode with you regularly but haven't shown up in 6+ weeks._",
        "atrisk_footer":  "\n_💬 Consider reaching out — a personal message goes a long way._",
        "atrisk_absent":  "absent {w}w",
        "rides_together": "rides together",

        # ── Loyal ────────────────────────────────────────────────────────────
        "loyal_none":       "🏅 No loyal active members found yet.",
        "loyal_header":     "*🏅 Most loyal active members ({n})*",
        "loyal_sub":        "_Ranked by rides together — still showing up regularly._",
        "loyal_rides":      "{n} rides",
        "loyal_rider_tier": " ({tier} rider)",

        # ── Briefing ─────────────────────────────────────────────────────────
        "briefing_stat_events":   "events",
        "briefing_stat_members":  "community members",
        "briefing_stat_profiles": "full profiles",
        "briefing_stat_bikes":    "with bike gear identified",
        "briefing_no_year":       "No year data yet",
        "briefing_top_year":      "Top {year}:",
        "briefing_loyal_header":  "\n*🏅 Most loyal members (active)*",
        "briefing_upg_header":    "\n*🚲 Upgrade ({n})*",
        "briefing_upg_none":      "\nUpgrade — none flagged",
        "briefing_svc_header":    "\n*🔧 Service Due ({n})*",
        "briefing_svc_clear":     "\nService — all clear",
        "briefing_risk_header":   "\n*👥 Loyal riders going quiet ({n}) — not seen 6w+*",
        "briefing_risk_ok":       "\n👥 All regulars still showing up ✅",
        "briefing_upcoming":      "\n*📅 Upcoming events*",
        "briefing_brands":        "\n*🚲 Main bike brands*",

        # ── Athlete profile ──────────────────────────────────────────────────
        "profile_att_footer":    "Full profile coming soon.",
        "profile_upg_top":       "Upgrade opportunity — serious rider, bike below their level.",
        "profile_upg_mid":       "Potential upgrade — regular rider on entry bike.",
        "profile_not_found":     "No data found for {name} — not in the event attendance records.",
        "profile_ambiguous":     "Found {n} athletes matching *{name}* — please be more specific:\n{names}",
        "profile_upcoming":      "upcoming",
        "profile_last":          "last",
        "profile_km_wk":         "{wk:.0f} km/wk",
        "profile_km_lifetime":   "{km:,.0f}km lifetime",
        "profile_kmh_avg":       "{sp:.1f} km/h avg",
        "profile_longest":       "longest {km:.0f}km",
        "profile_km_on_bike":    "{km:,.0f}km on bike",
        "profile_bought":        "bought {src} {date} (est.)",
        "profile_bought_nodate": "bought {src} (est.)",
        "profile_events":        "{n} events",
        "profile_first":         "first {date}",
        "profile_svc_alert":     "service due ({km:,.0f}km since last)",
        "profile_chn_alert":     "chain due ({km:,.0f}km since last)",

        # ── Draft ────────────────────────────────────────────────────────────
        "draft_header":       "*Draft for {first}* ({reason})\n\n{draft}\n\n",
        "draft_footer":       "_(copy & send on WhatsApp)_",
        "draft_no_events":    (
            "*{first} has never attended a club ride.*\n\n"
            "If he/she is a potential recruit — use *recruit* to see all solo riders "
            "or draft a personal invitation instead:\n"
            "_\"draft for {first}, invite them to join our next group ride\"_"
        ),
        "draft_not_found":    "No data found for '{name}' — not in the event attendance records.",
        "draft_ambiguous":    "Found {n} athletes matching *{name}* — please be more specific:\n{names}",
        "draft_reason_upg":   "upgrade candidate · {wk:.0f}km/wk · {bk_km:,.0f}km on {bike}",
        "draft_reason_svc":   "service due · {km:,.0f}km since last",
        "draft_reason_chn":   "chain due · {km:,.0f}km since last",
        "draft_reason_risk":  "inactive · {w}w since last seen",
        "draft_reason_eng":   "general engagement",
        "draft_fallback_svc": (
            "Hey {first}, your bike is coming up on {km:,.0f}km — "
            "good time to bring it in for a service before summer."
        ),
        "draft_fallback_upg": (
            "Hey {first}, you've been putting in serious km lately — "
            "we have some great new bikes in the shop worth a look."
        ),
        "draft_fallback_ghost": (
            "Hey {first}, haven't seen you at the club rides in a while — "
            "hope everything's good, would love to have you back."
        ),
        "draft_fallback_generic": (
            "Hey {first}, haven't seen you around lately — "
            "would love to catch up at the shop."
        ),

        # ── Missed upgrades ──────────────────────────────────────────────────
        "missed_none":    "No recent upgrades detected — all clear.",
        "missed_header":  "🎯 *Missed Upgrades ({n})*",
        "missed_sub":     "_Riders who recently upgraded — not through the shop_",
        "missed_new":     "New",
        "missed_was":     "Was",

        # ── Feedback replies ─────────────────────────────────────────────────
        "feedback_logged":     "Got it — logged '{action}' for {athlete}.",
        "feedback_no_context": "Reply 1-4 after an alert, or ask me anything about the club.",

        # ── Errors ───────────────────────────────────────────────────────────
        "busy":  "Service temporarily busy — please try again in a moment.",
        "error": "Something went wrong. Please try again.",

        # ── LLM language instruction ─────────────────────────────────────────
        "llm_lang": "Respond in English.",
    },

    # ══════════════════════════════════════════════════════════════════════════
    "fr": {

        # ── Language toggle ──────────────────────────────────────────────────
        "lang_set_en": "Basculé en anglais. 🇬🇧",
        "lang_set_fr": "Basculé en français. 🇫🇷",

        # ── Help ─────────────────────────────────────────────────────────────
        "help": (
            "*TNCE | ClubRide.Ai* — ton assistant club\n"
            "\n"
            "🏆 *top 10* — classement de la semaine\n"
            "🎯 *qui contacter* — 2 priorités du week-end\n"
            "🚲 *upgrade* — cyclistes prêts pour un nouveau vélo\n"
            "🔧 *service* — vélos en retard d'entretien ou de chaîne\n"
            "⚠️ *at risk* — membres fidèles qui s'éloignent\n"
            "👥 *recruit* — cyclistes solo à inviter\n"
            "🏅 *loyal* — membres les plus actifs\n"
            "📋 *briefing* — rapport hebdomadaire complet\n"
            "\n"
            "*Profil cycliste :*\n"
            "👤 \"dis-moi pour Marko\"\n"
            "👤 \"qui est Julien\"\n"
            "\n"
            "*Messages personnalisés :*\n"
            "✉️ \"draft for [nom]\" — détecte le signal (upgrade / service / réengagement)\n"
            "✉️ \"draft for [nom], mention [ta note]\" — ajoute ton angle\n"
            "\n"
            "*Langue :*\n"
            "🇫🇷 écris en français → réponses en français\n"
            "🇬🇧 type in English → replies in English\n"
        ),

        # ── Leaderboard ──────────────────────────────────────────────────────
        "lb_no_data":      "Aucune donnée de classement disponible.",
        "lb_no_community": "Aucun membre de la communauté dans le classement W{week}/{year}.",
        "lb_came":         "est venu à la sortie club",
        "lb_registered":   "inscrits pour",

        # ── Service ──────────────────────────────────────────────────────────
        "svc_no_alerts":    "Aucune alerte service ou chaîne — tout est bon.",
        "svc_due_header":   "Service dû ({n}) *[données estimées]*",
        "chain_due_header": "\nChaîne à remplacer ({n}) *[données estimées]*",
        "svc_since":        "km depuis entretien",
        "chain_since":      "km depuis chaîne",

        # ── Upgrade ──────────────────────────────────────────────────────────
        "upg_none":        "Aucun candidat upgrade pour le moment.",
        "upg_header":      "Candidats upgrade ({n})",
        "upg_km_wk":       "{wk:.0f} km/sem",
        "upg_km_wk_est":   "~{wk:.0f} km/sem (est.)",
        "upg_bike_unk":    "vélo inconnu",
        "upg_km_on_bike":  "{km:,.0f}km sur ce vélo",
        "upg_bought_tnce": "acheté TNCE {yr} (est.)",
        "upg_bought_ext":  "acheté externe (est.)",
        "upg_solo":        "cycliste solo",
        "upg_occasional":  "occasionnel ({n} sorties)",
        "upg_regular":     "régulier ({n} sorties)",
        "upg_loyal":       "fidèle ({n} sorties)",

        # ── Weekend priorities ───────────────────────────────────────────────
        "wp_none":   "Aucune priorité ce week-end — tout est bon.",
        "wp_header": "*AGIR CE WEEK-END*",

        # ── Recruits ─────────────────────────────────────────────────────────
        "rec_none":    "Aucun cycliste solo à recruter pour le moment.",
        "rec_header":  "Candidats recrutement ({n}) — cyclistes sérieux qui n'ont jamais rejoint une sortie",
        "rec_footer":  "\nCes cyclistes sont actifs mais roulent seuls — mérite une invitation personnelle.",
        "rec_no_bike": "vélo inconnu",
        "rec_km_wk":   "{km:.0f} km/sem",
        "rec_tier":    "niveau {tier}",
        "rec_rider":   "cycliste {tier}",

        # ── At risk ──────────────────────────────────────────────────────────
        "atrisk_none":    "👥 Tous les habitués sont encore là — personne à risque.",
        "atrisk_header":  "👥 *Membres fidèles qui s'éloignent ({n})*",
        "atrisk_sub":     "_Ces membres roulaient régulièrement avec toi mais ne se sont pas montrés depuis 6+ semaines._",
        "atrisk_footer":  "\n_💬 Pense à les contacter — un message personnel fait beaucoup._",
        "atrisk_absent":  "absent {w}s",
        "rides_together": "sorties ensemble",

        # ── Loyal ────────────────────────────────────────────────────────────
        "loyal_none":       "🏅 Aucun membre fidèle actif trouvé.",
        "loyal_header":     "*🏅 Membres les plus fidèles ({n})*",
        "loyal_sub":        "_Classés par sorties ensemble — encore là régulièrement._",
        "loyal_rides":      "{n} sorties",
        "loyal_rider_tier": " ({tier} cycliste)",

        # ── Briefing ─────────────────────────────────────────────────────────
        "briefing_stat_events":   "sorties",
        "briefing_stat_members":  "membres communauté",
        "briefing_stat_profiles": "profils complets",
        "briefing_stat_bikes":    "avec équipement identifié",
        "briefing_no_year":       "Pas encore de données annuelles",
        "briefing_top_year":      "Top {year} :",
        "briefing_loyal_header":  "\n*🏅 Membres les plus fidèles (actifs)*",
        "briefing_upg_header":    "\n*🚲 Upgrade ({n})*",
        "briefing_upg_none":      "\nUpgrade — aucun signalé",
        "briefing_svc_header":    "\n*🔧 Service dû ({n})*",
        "briefing_svc_clear":     "\nService — tout est bon",
        "briefing_risk_header":   "\n*👥 Membres fidèles qui s'éloignent ({n}) — absents 6s+*",
        "briefing_risk_ok":       "\n👥 Tous les habitués encore là ✅",
        "briefing_upcoming":      "\n*📅 Prochains événements*",
        "briefing_brands":        "\n*🚲 Principales marques de vélos*",

        # ── Athlete profile ──────────────────────────────────────────────────
        "profile_att_footer":    "Profil complet bientôt disponible.",
        "profile_upg_top":       "Opportunité upgrade — cycliste sérieux, vélo en dessous de son niveau.",
        "profile_upg_mid":       "Upgrade potentiel — cycliste régulier sur vélo entrée de gamme.",
        "profile_not_found":     "Aucune donnée trouvée pour {name} — pas dans les records de présence.",
        "profile_ambiguous":     "Trouvé {n} athlètes correspondant à *{name}* — sois plus précis :\n{names}",
        "profile_upcoming":      "prochain",
        "profile_last":          "dernier",
        "profile_km_wk":         "{wk:.0f} km/sem",
        "profile_km_lifetime":   "{km:,.0f}km total",
        "profile_kmh_avg":       "{sp:.1f} km/h moy.",
        "profile_longest":       "plus longue {km:.0f}km",
        "profile_km_on_bike":    "{km:,.0f}km sur le vélo",
        "profile_bought":        "acheté {src} {date} (est.)",
        "profile_bought_nodate": "acheté {src} (est.)",
        "profile_events":        "{n} sorties club",
        "profile_first":         "première fois {date}",
        "profile_svc_alert":     "service dû ({km:,.0f}km depuis dernier)",
        "profile_chn_alert":     "chaîne due ({km:,.0f}km depuis dernière)",

        # ── Draft ────────────────────────────────────────────────────────────
        "draft_header":       "*Message pour {first}* ({reason})\n\n{draft}\n\n",
        "draft_footer":       "_(copie et envoie sur WhatsApp)_",
        "draft_no_events":    (
            "*{first} n'a jamais participé à une sortie club.*\n\n"
            "S'il/elle est un recrutement potentiel — utilise *recruit* pour voir tous les "
            "cyclistes solo ou rédige une invitation :\n"
            "_\"draft for {first}, invite them to join our next group ride\"_"
        ),
        "draft_not_found":    "Aucune donnée trouvée pour '{name}' — pas dans les records de présence.",
        "draft_ambiguous":    "Trouvé {n} athlètes correspondant à *{name}* — sois plus précis :\n{names}",
        "draft_reason_upg":   "candidat upgrade · {wk:.0f}km/sem · {bk_km:,.0f}km sur {bike}",
        "draft_reason_svc":   "service dû · {km:,.0f}km depuis dernier",
        "draft_reason_chn":   "chaîne due · {km:,.0f}km depuis dernière",
        "draft_reason_risk":  "inactif · {w}s depuis dernière apparition",
        "draft_reason_eng":   "réengagement général",
        "draft_fallback_svc": (
            "Hey {first}, ton vélo approche les {km:,.0f}km — "
            "bon moment de l'amener pour un entretien avant l'été."
        ),
        "draft_fallback_upg": (
            "Hey {first}, tu mets des km sérieux ces temps — "
            "on a de super nouveaux vélos en boutique à voir."
        ),
        "draft_fallback_ghost": (
            "Hey {first}, on ne t'a plus vu aux sorties club — "
            "j'espère que tout va, on t'attend avec plaisir."
        ),
        "draft_fallback_generic": (
            "Hey {first}, on ne t'a plus vu récemment — "
            "viens donc faire un tour en boutique."
        ),

        # ── Missed upgrades ──────────────────────────────────────────────────
        "missed_none":    "Aucun upgrade récent détecté — tout est bon.",
        "missed_header":  "🎯 *Upgrades manqués ({n})*",
        "missed_sub":     "_Cyclistes qui ont récemment upgradé — pas via la boutique_",
        "missed_new":     "Nouveau",
        "missed_was":     "Avant",

        # ── Feedback replies ─────────────────────────────────────────────────
        "feedback_logged":     "Noté — '{action}' enregistré pour {athlete}.",
        "feedback_no_context": "Réponds 1-4 après une alerte, ou pose une question sur le club.",

        # ── Errors ───────────────────────────────────────────────────────────
        "busy":  "Service temporairement occupé — réessaie dans un moment.",
        "error": "Quelque chose s'est mal passé. Réessaie.",

        # ── LLM language instruction ─────────────────────────────────────────
        "llm_lang": "Réponds en français.",
    },
}


def t(key: str, lang: str = "en", **kwargs) -> str:
    """
    Fetch a translated string. Falls back to EN if the key or lang is missing.
    Supports template substitution via keyword args: t("upg_header", lang, n=5).
    """
    lang = lang if lang in S else "en"
    text = S[lang].get(key) or S["en"].get(key) or key
    if not kwargs:
        return text
    try:
        return text.format(**kwargs)
    except (KeyError, IndexError):
        return text
