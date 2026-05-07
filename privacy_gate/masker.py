"""
privacy_gate/masker.py
Anonymises athlete names before any text reaches Gemini.

Strategy:
  - Each known athlete gets a stable token: ATH_000 … ATH_NNN
  - Incoming owner messages are scanned for name matches via Levenshtein distance
  - Tokens are resolved back to real names after Gemini returns the tool call
  - Token map lives for one handle() call — no persistence, no state

Levenshtein threshold (per normalised name length):
  ≤ 5 chars  → distance ≤ 1   short names, strict to avoid false positives
  6–10 chars → distance ≤ 2   medium names, handles missing accents
  > 10 chars → distance ≤ 3   long names, tolerates typos
"""

import re
import unicodedata


# ── Text normalisation ─────────────────────────────────────────────────────────

def _norm(name: str) -> str:
    """Lowercase + strip accents + strip emoji/symbols."""
    name = re.sub(r"\(.*?\)", "", str(name))
    truncated = []
    for c in name:
        if unicodedata.category(c) in ("So", "Cs", "Co", "Cn"):
            break
        truncated.append(c)
    name = "".join(truncated)
    nfkd = unicodedata.normalize("NFD", name)
    name = "".join(
        c for c in nfkd
        if unicodedata.category(c) in ("Ll", "Lu", "Mn", "Zs") or c in ("-", "'")
    )
    return re.sub(r"\s+", " ", name).strip().lower()


# ── Levenshtein distance ───────────────────────────────────────────────────────

def _levenshtein(a: str, b: str) -> int:
    if len(a) < len(b):
        return _levenshtein(b, a)
    if not b:
        return len(a)
    row = list(range(len(b) + 1))
    for ca in a:
        new_row = [row[0] + 1]
        for j, cb in enumerate(b, 1):
            new_row.append(min(
                row[j] + 1,
                new_row[j - 1] + 1,
                row[j - 1] + (ca != cb)
            ))
        row = new_row
    return row[-1]


def _threshold(norm_name: str) -> int:
    n = len(norm_name)
    if n <= 5:  return 1
    if n <= 10: return 2
    return 3


# ── Anonymiser ─────────────────────────────────────────────────────────────────

class NameAnonymiser:
    """
    Builds a token map from a list of known athlete names.
    Use anonymise() before sending text to Gemini.
    Use deanonymise() after receiving the tool call back.
    """

    def __init__(self, names: list[str]):
        self._token_to_real: dict[str, str] = {}
        self._norm_to_token: dict[str, str] = {}

        for i, name in enumerate(names):
            token = f"ATH_{i:03d}"
            self._token_to_real[token] = name
            self._norm_to_token[_norm(name)] = token

        # Longest names first — greedy matching avoids partial replacements
        self._sorted_norms = sorted(
            self._norm_to_token.keys(), key=len, reverse=True
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def anonymise(self, text: str) -> str:
        """Replace known athlete names in text with ATH_XXX tokens."""
        if not text or not self._sorted_norms:
            return text

        result = text
        for norm_name in self._sorted_norms:
            token = self._norm_to_token[norm_name]
            n_words = len(norm_name.split())
            result = self._replace_in_text(result, norm_name, token, n_words)

        return result

    def deanonymise(self, value: str) -> str:
        """Convert ATH_XXX token back to real name. Returns value unchanged if not a token."""
        return self._token_to_real.get(value.strip(), value)

    def deanonymise_args(self, args: dict) -> dict:
        """De-anonymise athlete_name field in Gemini tool call args."""
        if "athlete_name" in args:
            args = dict(args)
            args["athlete_name"] = self.deanonymise(args["athlete_name"])
        return args

    # ── Internal ───────────────────────────────────────────────────────────────

    def _replace_in_text(self, text: str, norm_name: str,
                          token: str, n_words: int) -> str:
        """Sliding window match — replaces first occurrence per name."""
        words = text.split()
        result = []
        i = 0
        while i < len(words):
            chunk = words[i: i + n_words]
            if len(chunk) < n_words:
                result.append(words[i])
                i += 1
                continue

            candidate_norm = _norm(" ".join(chunk))
            dist = _levenshtein(candidate_norm, norm_name)

            if dist <= _threshold(norm_name):
                result.append(token)
                i += n_words
            else:
                result.append(words[i])
                i += 1

        return " ".join(result)


# ── Factory ────────────────────────────────────────────────────────────────────

def build_anonymiser(names: list[str]) -> NameAnonymiser:
    """Build an anonymiser from a list of athlete names. Empty list = passthrough."""
    return NameAnonymiser([n for n in names if n and str(n) != "nan"])
