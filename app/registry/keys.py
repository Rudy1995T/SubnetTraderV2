"""Occupant keys (master §5): ``occupant_key = f"{netuid}:{network_registered_at}"``."""
from __future__ import annotations


def occupant_key(netuid: int, network_registered_at: int) -> str:
    return f"{int(netuid)}:{int(network_registered_at)}"


def parse_occupant_key(key: str) -> tuple[int, int]:
    a, sep, b = str(key).partition(":")
    if not sep:
        raise ValueError(f"bad occupant_key {key!r}")
    return int(a), int(b)
