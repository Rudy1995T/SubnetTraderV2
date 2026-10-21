"""Golden hashes of the frozen scoring specs (spec 12 §2.13). Re-exported by ``app.scoring.spec``; kept in their own
module so the registry's import-time check can read them without importing the spec (which imports the registry).

A mismatch fails boot on purpose (§4 case 20): editing ts-1.0 needs a new ``spec_version``, never a new hash here.
"""
from __future__ import annotations

from typing import Final

GOLDEN_REGISTRY_HASH: Final = {"ts-1.0": "8d0e7d0f845ea6082cdc00c5e1fc071cd6b3e0c3493b210334003e7c151a6e20"}
GOLDEN_SPEC_HASH: Final = {"ts-1.0": "1f24aa8068057b7efdc1f05b054509a1ca07fd770c61196cd3304225132b6089"}
