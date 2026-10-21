"""``rules:v2`` lexicon and event regexes (spec 10 §8.4). Frozen with ``RULES_VERSION``: any edit here is a new rules
version (and a new ``LABEL_POLICY`` fallback entry).

Polarity terms are crypto-tuned single tokens (or short phrases) with weights 1..3; the event regexes are tried in the
listed priority order, first match wins.
"""
from __future__ import annotations

import re

POS: dict[str, float] = {
    # shipping / product
    "ship": 2, "shipped": 2, "shipping": 2, "live": 2, "launch": 2, "launched": 2, "launching": 2, "release": 1,
    "released": 2, "mainnet": 2, "deploy": 1, "deployed": 2, "rollout": 1, "unveil": 2, "unveiled": 2, "beta": 1,
    "upgrade": 1, "upgraded": 1, "improved": 1, "improvement": 1, "faster": 1, "integrated": 2, "integration": 2,
    "integrates": 2, "available": 1, "milestone": 2, "delivered": 2, "completed": 1, "achieved": 2, "record": 2,
    "sota": 1, "breakthrough": 2, "outperforms": 2, "beats": 2, "leading": 1, "first": 1, "new": 1,
    # business
    "revenue": 2, "profit": 2, "profitable": 2, "arr": 2, "mrr": 2, "customers": 2, "customer": 1, "paying": 2,
    "users": 1, "adoption": 2, "demand": 1, "usage": 1, "growth": 2, "growing": 2, "grew": 2, "scale": 1,
    "scaling": 1, "partnership": 2, "partner": 2, "partners": 2, "partnered": 2, "collaboration": 1,
    "deal": 1, "contract": 1, "enterprise": 1, "funding": 2, "raised": 2, "raise": 1, "backed": 2, "invest": 1,
    "investment": 1, "grant": 1, "buyback": 3, "buybacks": 3, "burn": 2, "burned": 2, "listed": 2, "listing": 2,
    "hire": 1, "hired": 1, "joins": 1, "welcome": 1,
    # sentiment
    "bullish": 3, "moon": 2, "mooning": 2, "pump": 1, "pumping": 1, "rally": 2, "ath": 2, "breakout": 2,
    "undervalued": 2, "gem": 2, "strong": 2, "strength": 1, "solid": 1, "great": 2, "amazing": 3, "awesome": 3,
    "excellent": 3, "incredible": 3, "impressive": 2, "excited": 2, "exciting": 2, "love": 2, "best": 2, "win": 2,
    "winning": 2, "winner": 2, "success": 2, "successful": 2, "congrats": 2, "congratulations": 2, "proud": 2,
    "thrilled": 3, "huge": 1, "massive": 1, "insane": 1, "top": 1, "up": 1, "higher": 1, "gain": 1, "gains": 1,
    "outperform": 2, "accumulate": 1, "accumulating": 1, "conviction": 1, "legit": 1, "real": 1, "fire": 1,
    "lfg": 2, "wagmi": 2, "based": 1, "hot": 1, "trending": 1, "momentum": 1, "recovering": 1,
    "recovery": 1, "resilient": 1, "secure": 1, "audited": 2, "transparent": 1, "open-source": 1, "opensource": 1,
}
NEG: dict[str, float] = {
    # security / integrity
    "exploit": 3, "exploited": 3, "hack": 3, "hacked": 3, "hacker": 3, "drained": 3, "drain": 2, "stolen": 3,
    "theft": 3, "compromised": 3, "compromise": 3, "breach": 3, "vulnerability": 2, "phishing": 2, "phish": 2,
    "scam": 3, "scammer": 3, "fraud": 3, "fake": 2, "rug": 3, "rugged": 3, "rugpull": 3, "ponzi": 3, "exit": 1,
    "attack": 2, "attacked": 2, "malicious": 2, "backdoor": 3, "leak": 2, "leaked": 2,
    # owner / tokenomics
    "dump": 3, "dumped": 3, "dumping": 3, "sold": 1, "selling": 1, "sell": 1, "unlock": 1, "extraction": 3,
    "extract": 2, "dilution": 2, "inflation": 1, "copied": 2, "copy": 1, "plagiarized": 3, "inherited": 1,
    "weight-copying": 3, "cheating": 3, "cheat": 3, "exploiting": 3,
    # delivery / team
    "delay": 2, "delayed": 2, "postponed": 2, "missed": 2, "slipped": 2, "behind": 1, "abandon": 3, "abandoned": 3,
    "abandoning": 3, "quit": 2, "resign": 2, "resigned": 2, "leaving": 1, "departure": 2, "stepping": 1,
    "shutdown": 3, "shutting": 2, "discontinued": 3, "deprecated": 2, "sunset": 2, "halted": 2, "paused": 2,
    "pause": 1, "outage": 2, "down": 1, "broken": 2, "bug": 1, "bugs": 1, "issue": 1, "issues": 1, "failed": 2,
    "failure": 2, "fail": 2, "failing": 2, "crash": 2, "crashed": 2, "error": 1, "errors": 1, "downtime": 2,
    "expired": 1, "deregistered": 3, "dereg": 2, "deregistration": 2,
    # governance / legal
    "banned": 3, "ban": 2, "ousted": 3, "removed": 2, "dispute": 2, "conflict": 2, "lawsuit": 3, "sued": 3,
    "sec": 1, "investigation": 2, "regulatory": 1, "illegal": 3, "warning": 1, "beware": 2, "caution": 1,
    # sentiment
    "bearish": 3, "plunge": 2, "plunged": 2, "tank": 2, "tanked": 2, "collapse": 3, "collapsed": 3,
    "dead": 3, "dying": 3, "worthless": 3, "terrible": 3, "awful": 3, "bad": 2, "worse": 2, "worst": 3,
    "disappointing": 2, "disappointed": 2, "concern": 1, "concerns": 1, "concerning": 2, "worried": 2, "risk": 1,
    "risky": 2, "red": 1, "lower": 1, "loss": 2, "losses": 2, "lost": 2, "overvalued": 2, "bleeding": 2, "ngmi": 2,
    "rekt": 3, "fud": 1, "sus": 2, "shady": 3, "problem": 1, "problems": 1, "weak": 2, "slow": 1,
}
NEGATORS = frozenset({"not", "no", "never", "isn't", "isnt", "aren't", "arent", "wasn't", "wasnt", "don't", "dont",
                      "doesn't", "doesnt", "didn't", "didnt", "won't", "wont", "can't", "cant", "without", "nor"})
BOOSTERS: dict[str, float] = {"huge": 1.5, "massive": 1.5, "very": 1.3, "extremely": 1.5, "super": 1.3,
                              "incredibly": 1.5, "really": 1.2, "major": 1.3, "absolutely": 1.4}
EMOJI: dict[str, float] = {"🚀": 2, "🔥": 1, "💎": 1, "📈": 1, "✅": 1, "🎉": 1, "💪": 1, "🙌": 1,
                           "💀": -2, "🚩": -2, "📉": -1, "⚠️": -1, "⚠": -1, "😱": -1, "🤡": -2, "❌": -1}
NEG_WINDOW = 3
VADER_ALPHA = 15.0
POS_THRESHOLD = 0.15

_F = re.I
_CEX = r"kraken|coinbase|binance|okx|bybit|gate\.io|kucoin|mexc"
# First match wins (10 §8.4 order, widened 2026-10-18 after the 40-item golden run: macro-F1 0.442 → see BUILD-LOG).
EVENT_RULES: tuple[tuple[str, re.Pattern], ...] = (
    ("security_incident", re.compile(r"(was|been|got|were) (hacked|exploited|drained|compromised)|\bhacked\b"
                                     r"|exploited\b|\bexploit (of|on|in|against)\b|drained|compromis(ed|e of)"
                                     r"|phish|key.?(leak|stolen)|social(ly)?.?engineer", _F)),   # not "preventing exploits"
    ("owner_extraction_evidence", re.compile(r"owner.{0,60}(dump|sold|sell|unlock|extract|unstak|moved .{0,30}exchange)"
                                             r"|(dump|sold).{0,40}owner", _F)),
    ("token_buyback_burn", re.compile(r"buy.?back|burn(ed|ing)?\b", _F)),
    ("exchange_listing", re.compile(rf"(list(s|ed|ing)?|live|trading|perpetuals?|spot)\b.{{0,40}}\b({_CEX})\b"
                                    rf"|\b({_CEX})\b.{{0,20}}\blists?\b|\bdex\b|aggregator", _F)),
    ("emissions_tokenomics_change", re.compile(r"emissions?\b.{0,20}\b(cut|change|drop|halv|reduc|increas)"
                                               r"|halving|tokenomics", _F)),
    ("revenue_customer", re.compile(r"\$[\d.,]+ ?(k|m)? (arr|revenue|mrr)|paying customers|revenue"
                                    r"|enterprise customer|signs? .{0,30}customer", _F)),
    ("delay_missed_milestone", re.compile(r"delay(ed)?|postpon|pushed back|slipp|\bslips?\b|missed (the|our|its)", _F)),
    ("team_change", re.compile(r"step(ping)? (down|away)|leav(e|ing) the team|joins? as|new (cto|ceo|lead)"
                               r"|abandon(ed|s)?\b|team (left|quit)", _F)),
    ("governance_dispute", re.compile(r"banned|ousted|dispute|timeout|removed from|accus|weight.?copying", _F)),
    ("regulatory", re.compile(r"\bsec\b|wells notice|lawsuit|subpoena|regulator", _F)),
    ("product_launch", re.compile(r"(is|now) live|launch(ed|ing)?\b|mainnet|public beta", _F)),
    ("partnership_integration", re.compile(r"integrat(es|ed|ion)|partner(ship|s|ed|ing)?\b", _F)),
    ("release_upgrade", re.compile(r"v\d+\.\d+|release notes|changelog|upgrade|shipped", _F)),
    ("funding", re.compile(r"raise[sd]?|series [a-c]|seed round", _F)),
    ("giveaway_promo", re.compile(r"giveaway|airdrop|rt (and|&) follow|like (and|&) rt|to win\b|tag \d friends", _F)),
)
GIVEAWAY_RE = EVENT_RULES[-1][1]
NETUID_RE = re.compile(r"\b(?:sn|subnet|netuid)\s?#?(\d{1,3})\b|\$sn(\d{1,3})\b", _F)
CONTEXT_RE = re.compile(r"bittensor|\btao\b|subnet|netuid|\bsn\d", _F)
SHORTENER_RE = re.compile(r"\b(?:bit\.ly|tinyurl\.com|t\.co|goo\.gl|ow\.ly|is\.gd|buff\.ly|cutt\.ly|rb\.gy|"
                          r"shorturl\.at|tiny\.cc)/", _F)
SEND_TAO_RE = re.compile(r"send\s+(?:\d+\s*)?\$?tao", _F)
URL_RE = re.compile(r"https?://\S+|\bwww\.\S+", _F)
NUMBER_RE = re.compile(r"\d")
KEY_COMPROMISE_RE = re.compile(r"coldkey|hotkey|private key|seed phrase|mnemonic|compromis", _F)   # §3.2 review rule
TOKEN_RE = re.compile(r"[a-z0-9$][a-z0-9'\-]*", _F)

__all__ = ["POS", "NEG", "NEGATORS", "BOOSTERS", "EMOJI", "EVENT_RULES", "GIVEAWAY_RE", "NETUID_RE", "CONTEXT_RE",
           "SHORTENER_RE", "SEND_TAO_RE", "URL_RE", "NUMBER_RE", "KEY_COMPROMISE_RE", "TOKEN_RE", "NEG_WINDOW",
           "VADER_ALPHA", "POS_THRESHOLD"]
