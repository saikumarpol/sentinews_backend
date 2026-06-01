# backend/ai_processor.py
#
# Sentiment scoring and news processing for Sentinews.
# Upgraded from 6-word keyword list to full financial sentiment vocabulary.
# Also adds: impact scoring, event tagging, and watchlist-aware digest.

import re
from typing import List, Dict, Optional

# ── Sentiment vocabulary ───────────────────────────────────────────────────

_POSITIVE = [
    # price action
    "rises", "surge", "rally", "gain", "up", "high", "record", "bullish",
    "breakout", "all-time high", "52-week high", "outperform", "beat",
    "beats", "strong", "upside", "upgrade",
    # fundamentals
    "profit", "earnings beat", "revenue growth", "margin expansion",
    "improving", "turnaround", "debt-free", "dividend", "buyback",
    "acquisition", "expansion", "capex", "order win", "contract win",
    "partnership", "joint venture", "fpo", "ipo success",
    # macro
    "rate cut", "stimulus", "gdp growth", "fii buying", "dii buying",
    "inflows", "foreign inflow", "positive sentiment", "recovery",
    "easing", "liquidity", "rbi support",
]

_NEGATIVE = [
    # price action
    "falls", "drop", "crash", "slump", "decline", "down", "low", "bearish",
    "sell-off", "correction", "underperform", "miss", "weak", "downside",
    "downgrade", "all-time low", "52-week low",
    # fundamentals
    "loss", "earnings miss", "revenue decline", "margin pressure",
    "debt", "default", "fraud", "probe", "investigation", "fine",
    "penalty", "insolvency", "bankruptcy", "write-off", "impairment",
    "layoffs", "restructuring",
    # macro
    "rate hike", "inflation", "recession", "stagflation", "fii selling",
    "outflows", "foreign outflow", "negative sentiment", "slowdown",
    "tightening", "credit risk", "npa", "bad loans",
    # geopolitical
    "war", "sanctions", "tariff", "trade war", "geopolitical tension",
    "oil shock", "supply disruption",
]

# ── Event / Impact tagging ──────────────────────────────────────────────────

_EVENT_TAGS = [
    ("earnings",   ["earnings", "results", "profit", "revenue", "q1", "q2", "q3", "q4", "quarterly"]),
    ("ipo",        ["ipo", "listing", "public issue", "fpo", "primary market"]),
    ("rbi_policy", ["rbi", "repo rate", "monetary policy", "central bank", "rate cut", "rate hike"]),
    ("fii_dii",    ["fii", "dii", "foreign investor", "institutional", "fpi"]),
    ("merger_ma",  ["merger", "acquisition", "m&a", "takeover", "buyout", "stake"]),
    ("macro",      ["gdp", "cpi", "wpi", "inflation", "iip", "pmi", "trade deficit"]),
    ("oil_energy", ["crude", "petroleum", "opec", "natural gas", "oil price"]),
    ("budget_tax", ["budget", "tax", "gst", "fiscal", "government"]),
    ("sebi_reg",   ["sebi", "regulator", "circular", "compliance", "norms"]),
]

_IMPACT_HIGH = [
    "rbi", "sebi", "budget", "rate cut", "rate hike", "crash", "circuit",
    "earnings miss", "earnings beat", "fraud", "probe", "record high",
    "all-time", "ipo listing", "gdp", "inflation",
]

_IMPACT_MED = [
    "results", "profit", "revenue", "dividend", "merger", "fii", "dii",
    "upgrade", "downgrade", "order win", "expansion", "partnership",
]


def _score_sentiment(text: str) -> float:
    """
    Score a news item. Returns float in [-1.0, +1.0].
    Weighted: exact phrase > single word.
    """
    lower = text.lower()
    pos = 0.0
    neg = 0.0

    for phrase in _POSITIVE:
        if phrase in lower:
            weight = 1.5 if " " in phrase else 1.0   # phrases score higher
            pos += weight

    for phrase in _NEGATIVE:
        if phrase in lower:
            weight = 1.5 if " " in phrase else 1.0
            neg += weight

    raw = pos - neg
    if raw == 0:
        return 0.0
    # Normalise to ±1 with soft cap
    score = raw / max(pos + neg, 1)
    return round(max(-1.0, min(1.0, score)), 3)


def _tag_events(text: str) -> List[str]:
    lower = text.lower()
    tags = []
    for tag, keywords in _EVENT_TAGS:
        if any(kw in lower for kw in keywords):
            tags.append(tag)
    return tags


def _score_impact(text: str) -> str:
    lower = text.lower()
    if any(kw in lower for kw in _IMPACT_HIGH):
        return "high"
    if any(kw in lower for kw in _IMPACT_MED):
        return "medium"
    return "low"


def _action_from_sentiment(score: float) -> str:
    if score >= 0.3:
        return "BULLISH"
    if score >= 0.1:
        return "MILDLY BULLISH"
    if score <= -0.3:
        return "BEARISH"
    if score <= -0.1:
        return "MILDLY BEARISH"
    return "NEUTRAL"


def enrich_news_item(item: Dict) -> Dict:
    """Add sentiment, action, impact_score, event_tags to a single news item."""
    text = f"{item.get('headline', '')} {item.get('summary', '')}"
    sentiment  = _score_sentiment(text)
    return {
        **item,
        "sentiment":    sentiment,
        "action":       _action_from_sentiment(sentiment),
        "impact":       _score_impact(text),
        "event_tags":   _tag_events(text),
    }


def enrich_news_batch(items: List[Dict]) -> List[Dict]:
    """Enrich a list of news items with sentiment, action, impact, and event tags."""
    return [enrich_news_item(i) for i in items]


# ── Watchlist digest ────────────────────────────────────────────────────────

def basic_sentiment_from_headline(headline: str) -> float:
    """Backwards-compatible wrapper — used by existing digest endpoint."""
    return _score_sentiment(headline)


def summarize_news_for_watchlist(
    news_items: List[Dict],
    watchlist: Optional[List[str]] = None,
) -> List[Dict]:
    """
    Filter news by watchlist tickers and add sentiment/action/impact.
    If watchlist is empty, return everything (enriched).
    """
    if watchlist is None:
        watchlist = []

    wl_upper = [w.upper() for w in watchlist]
    digest = []

    for item in news_items:
        tickers = [t.upper() for t in item.get("tickers", [])]
        if not wl_upper or any(t in wl_upper for t in tickers):
            digest.append(enrich_news_item(item))

    digest.sort(key=lambda x: x.get("published_at", ""), reverse=True)
    return digest
