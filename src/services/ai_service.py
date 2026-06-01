import re
from typing import List, Dict, Optional

_POSITIVE = [
    "rises", "surge", "rally", "gain", "up", "high", "record", "bullish",
    "breakout", "all-time high", "52-week high", "outperform", "beat",
    "beats", "strong", "upside", "upgrade", "profit", "earnings beat",
    "revenue growth", "margin expansion", "improving", "turnaround",
    "debt-free", "dividend", "buyback", "acquisition", "expansion",
    "capex", "order win", "contract win", "partnership", "joint venture",
    "fpo", "ipo success", "rate cut", "stimulus", "gdp growth", "fii buying",
    "dii buying", "inflows", "foreign inflow", "positive sentiment",
    "recovery", "easing", "liquidity", "rbi support",
]

_NEGATIVE = [
    "falls", "drop", "crash", "slump", "decline", "down", "low", "bearish",
    "sell-off", "correction", "underperform", "miss", "weak", "downside",
    "downgrade", "all-time low", "52-week low", "loss", "earnings miss",
    "revenue decline", "margin pressure", "debt", "default", "fraud",
    "probe", "investigation", "fine", "penalty", "insolvency", "bankruptcy",
    "write-off", "impairment", "layoffs", "restructuring", "rate hike",
    "inflation", "recession", "stagflation", "fii selling", "outflows",
    "foreign outflow", "negative sentiment", "slowdown", "tightening",
    "credit risk", "npa", "bad loans", "war", "sanctions", "tariff",
    "trade war", "geopolitical tension", "oil shock", "supply disruption",
]

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

def score_sentiment(text: str) -> float:
    lower = text.lower()
    pos = 0.0
    neg = 0.0
    for phrase in _POSITIVE:
        if phrase in lower:
            weight = 1.5 if " " in phrase else 1.0
            pos += weight
    for phrase in _NEGATIVE:
        if phrase in lower:
            weight = 1.5 if " " in phrase else 1.0
            neg += weight
    raw = pos - neg
    if raw == 0:
        return 0.0
    score = raw / max(pos + neg, 1)
    return round(max(-1.0, min(1.0, score)), 3)

def tag_events(text: str) -> List[str]:
    lower = text.lower()
    tags = []
    for tag, keywords in _EVENT_TAGS:
        if any(kw in lower for kw in keywords):
            tags.append(tag)
    return tags

def score_impact(text: str) -> str:
    lower = text.lower()
    if any(kw in lower for kw in _IMPACT_HIGH):
        return "high"
    if any(kw in lower for kw in _IMPACT_MED):
        return "medium"
    return "low"

def action_from_sentiment(score: float) -> str:
    if score >= 0.3: return "BULLISH"
    if score >= 0.1: return "MILDLY BULLISH"
    if score <= -0.3: return "BEARISH"
    if score <= -0.1: return "MILDLY BEARISH"
    return "NEUTRAL"

def enrich_news_item(item: Dict) -> Dict:
    text = f"{item.get('headline', '')} {item.get('summary', '')}"
    sentiment = score_sentiment(text)
    return {
        **item,
        "sentiment": sentiment,
        "action": action_from_sentiment(sentiment),
        "impact": score_impact(text),
        "event_tags": tag_events(text),
    }

def enrich_news_batch(items: List[Dict]) -> List[Dict]:
    return [enrich_news_item(i) for i in items]
