"""KOL seed list and the ambiguous-name stoplist (spec 09 §3.4, §2.5).

    python -m scripts.build_kol_list --write [PATH]      # write the embedded §3.4 table (default KOL_SEED_PATH)
    python -m scripts.build_kol_list --load [PATH]       # INSERT OR IGNORE into kol_accounts (operator edits kept)
    python -m scripts.build_kol_list --names             # regenerate app/nlp/ambiguous_names.txt (hand lines kept)

``--write`` and ``--load`` combine (``--write seed/kol_accounts.csv --load``). ``verified=1`` means the handle was
confirmed by search in research 01-web-ecosystem-2026 §4 (2026-09-23: 32 confirmed, 5 UNVERIFIED with credibility 0.0);
it does not vouch for role, credibility or team mapping. Netuids → occupant keys at load time via ``subnets``.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import re
import sys
from pathlib import Path

# (handle, display_name, category, credibility, verified, team_occupant_netuids, notes) — 09 §3.4, 37 rows
SEED: tuple[tuple[str, str, str, float, int, str, str], ...] = (
    ("opentensor", "OpenTensor Foundation", "official", 0.90, 1, "", "protocol foundation"),
    ("bittensor_", "Bittensor", "official", 0.90, 1, "", "protocol account"),
    ("taostats", "Taostats", "explorer", 0.85, 1, "", "canonical explorer"),
    ("learnbittensor", "Learn Bittensor", "official", 0.80, 1, "", "docs"),
    ("const_reborn", "Jacob Steeves (Const)", "founder", 0.85, 1, "120",
     "ex-OTF CEO; builds Affine SN120 — own-subnet posts excluded from KOL coverage"),
    ("mogmachine", "mogmachine", "founder", 0.75, 1, "", "Taostats founder"),
    ("MacrocosmosAI", "Macrocosmos", "founder", 0.70, 1, "1,9,13",
     "multi-subnet operator (SN25/SN37 re-registered as UR/Aurelius; checked 2026-10-18)"),
    ("manifoldlabs", "Manifold Labs", "founder", 0.70, 1, "4", "Targon"),
    ("taoshiio", "Taoshi", "founder", 0.60, 1, "8", "legacy SN8 operator; SN8 now Vanta — verify mapping"),
    ("webuildscore", "Score", "founder", 0.60, 1, "44", ""),
    ("_redteam_", "RedTeam", "founder", 0.60, 1, "61", ""),
    ("CrucibleLabs", "Crucible Labs", "founder", 0.55, 1, "", "subnet mapping unknown → operator"),
    ("QuasarModels", "Quasar", "founder", 0.30, 1, "24",
     "2026-07-31 SOTA claim, audited days later as 98 % inherited weights (early Aug 2026); low credibility by design"),
    ("YumaGroup", "Yuma (DCG)", "fund", 0.75, 1, "", "validator + fund; conflicted but informed"),
    ("DCGco", "Digital Currency Group", "fund", 0.60, 1, "", ""),
    ("BarrySilbert", "Barry Silbert", "fund", 0.60, 1, "", ""),
    ("xtaohq", "xTAO", "fund", 0.65, 1, "", "treasury co"),
    ("latentholdings", "Latent Holdings", "fund", 0.70, 1, "", "owns tao.app, Latent 11"),
    ("gtaoventures", "GTAO Ventures", "fund", 0.55, 1, "", ""),
    ("TrustedStake", "TrustedStake", "community", 0.55, 1, "", "validator"),
    ("0xSammy", "0xSammy", "analyst", 0.80, 1, "", "TAO Institute; 89k followers"),
    ("KeithSingery", "Keith Singery", "analyst", 0.75, 1, "", "Bittensor Guru podcast; validator"),
    ("here4impact", "Michael D. White", "analyst", 0.60, 1, "", ""),
    ("CryptoZPunisher", "CryptoZPunisher", "analyst", 0.55, 1, "", ""),
    ("bittingthembits", "Andy ττ", "analyst", 0.60, 1, "", "resource list maintainer"),
    ("tzedonn", "tzedonn", "analyst", 0.55, 1, "", ""),
    ("taomedia_", "tao.media", "media", 0.65, 1, "", "ecosystem outlet"),
    ("BSCNews", "BSC News", "media", 0.40, 1, "", "general crypto media; PR-heavy"),
    ("TAOCommunityHub", "TAO Community Hub", "community", 0.50, 1, "", ""),
    ("TAOFlows", "TAO Flows", "community", 0.50, 1, "", ""),
    ("proofoftalk", "Proof of Talk", "media", 0.35, 1, "",
     "conference account; verified per research §4, not in the brief's core list"),
    ("JesusMartinez", "(unknown)", "analyst", 0.35, 1, "",
     "listed verified in research §4 but role unclear → operator to confirm before raising credibility"),
    ("yuma_ai", "Yuma (alt)", "fund", 0.00, 0, "", "UNVERIFIED handle; may duplicate YumaGroup"),
    ("taoapp_", "tao.app", "explorer", 0.00, 0, "", "UNVERIFIED handle"),
    ("markjeffrey", "Mark Jeffrey (Stillcore)", "analyst", 0.00, 0, "", "UNVERIFIED handle"),
    ("TaoPortal", "TaoPortal", "community", 0.00, 0, "", "UNVERIFIED handle"),
    ("jaltucher", "James Altucher", "analyst", 0.00, 0, "", "UNVERIFIED handle (BlueTAO.ai)"),
)
COLUMNS = ("handle", "display_name", "category", "credibility", "verified", "team_occupant_netuids", "notes")

# 09 §2.5's curated ambiguity list (names that are ordinary words); ≤ 4-char names are ambiguous without listing
CURATED_AMBIGUOUS = ("Apex", "Score", "Nova", "Templar", "Gradients", "Vanta", "Synth", "Zeus", "Grail", "Forge",
                     "Base", "Its AI")
WORDS_FILES = ("/usr/share/dict/words", "/usr/share/dict/american-english", "/usr/share/dict/british-english")
NAMES_HEADER = """# Subnet names that are also ordinary words (09 §2.5 AMBIGUOUS_NAMES). A match on one of these needs a
# Bittensor co-term (bittensor | TAO | subnet | dTAO | netuid | SNnn) — in match_occupants and as required_coterms in mention
# search. Names of ≤ 4 characters are ambiguous without being listed. One name per line, compared case-insensitively.
# Regenerate with `python -m scripts.build_kol_list --names` (keeps hand-added lines)."""


def write_csv(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(COLUMNS)
        for h, dn, cat, cred, ver, nets, notes in SEED:
            w.writerow([h, dn, cat, f"{cred:.2f}", ver, nets, notes])
    return len(SEED)


def _dictionary() -> set[str]:
    for p in WORDS_FILES:
        try:
            return {w.strip().lower() for w in Path(p).read_text(encoding="utf-8", errors="ignore").splitlines()
                    if w.strip()}
        except OSError:
            continue
    return set()


def merge_names(existing_lines: list[str], registry_names: list[str], words: set[str]) -> list[str]:
    """Kept: every existing non-comment line (hand-added names are never dropped) ∪ the curated list ∪ registry names
    whose every word is a dictionary word (when a word list exists). Sorted case-insensitively, one per line."""
    from app.nlp.match import is_placeholder
    keep: dict[str, str] = {}
    for ln in existing_lines:
        s = ln.strip()
        if s and not s.startswith("#"):
            keep.setdefault(s.lower(), s)
    for n in CURATED_AMBIGUOUS:
        keep.setdefault(n.lower(), n)
    for n in registry_names:
        if not n or is_placeholder(n) or len(n.strip()) <= 4:
            continue
        toks = re.findall(r"[A-Za-z]+", n)
        if words and toks and all(t.lower() in words for t in toks) and len(toks) <= 3:
            keep.setdefault(n.strip().lower(), n.strip())
    return sorted(keep.values(), key=str.lower)


async def regenerate_names(db, path: Path) -> dict:
    names = [r["name"] for r in await db.fetchall("SELECT name FROM subnets WHERE netuid > 0 AND status <> "
                                                  "'deregistered' AND name IS NOT NULL")]
    existing = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    before = {x.strip().lower() for x in existing if x.strip() and not x.lstrip().startswith("#")}
    merged = merge_names(existing, names, _dictionary())
    path.write_text(NAMES_HEADER + "\n" + "\n".join(merged) + "\n", encoding="utf-8")
    return {"path": str(path), "names": len(merged), "added": sorted(n for n in merged if n.lower() not in before)}


async def _main(args: argparse.Namespace) -> int:
    import app.config as app_config
    s = app_config.settings
    out: dict = {}
    seed_path = Path(args.write) if isinstance(args.write, str) else s.path(s.KOL_SEED_PATH)
    if args.write is not None:
        out["written"] = {"path": str(seed_path), "rows": write_csv(seed_path)}
    if args.load is not None or args.names:
        from app.storage.db import Database
        db = Database(Path(args.db) if args.db else s.path(s.DB_PATH))
        await db.connect()
        await db.apply_schema()
        try:
            if args.load is not None:
                from app.collectors.x import load_kol_seed
                lp = Path(args.load) if isinstance(args.load, str) else seed_path
                out["loaded"] = await load_kol_seed(db, lp)
                row = await db.fetchone("SELECT COUNT(*) AS n, COALESCE(SUM(verified), 0) AS v FROM kol_accounts")
                out["kol_accounts"] = {"rows": int(row["n"]), "verified": int(row["v"])}
            if args.names:
                from app.nlp import match
                out["names"] = await regenerate_names(db, Path(match.__file__).with_name("ambiguous_names.txt"))
        finally:
            await db.close()
    print(json.dumps(out, ensure_ascii=False, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m scripts.build_kol_list", description="09 §3.4 KOL seed list")
    ap.add_argument("--write", nargs="?", const=True, default=None, metavar="PATH")
    ap.add_argument("--load", nargs="?", const=True, default=None, metavar="PATH")
    ap.add_argument("--names", action="store_true")
    ap.add_argument("--db", help="database path (default settings.DB_PATH)")
    args = ap.parse_args(argv)
    if args.write is None and args.load is None and not args.names:
        ap.print_help()
        return 2
    return asyncio.run(_main(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
