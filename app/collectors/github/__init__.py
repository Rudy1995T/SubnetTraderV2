"""GitHub collector (spec 06): canonical repo resolver, org-level activity, the ten P2 builder metrics.

Jobs: ``github_resolve``, ``github_org_enum``, ``github_activity``, ``github_metrics``, ``github_html_probe``
(``app.collectors.github.jobs``). ``get_client(rt)`` is the process-wide client 17's backfill shares.
"""
from __future__ import annotations

from app.collectors.github.client import get_client  # noqa: F401
