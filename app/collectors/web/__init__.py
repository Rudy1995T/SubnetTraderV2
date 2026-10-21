"""Web presence collector (spec 07): daily liveness / TLS / content checks of declared websites and docs, Wayback
site age, docs auto-discovery, and the six P3 metrics (``app.features.web_metrics``).

Jobs (01 ``JOB_TABLE`` factories ``collectors.web:…``): ``web_check``, ``web_metrics``, ``web_spn2``.
"""
from __future__ import annotations

from app.collectors.web.jobs import WebCheckJob, WebMetricsJob, WebSpn2Job  # noqa: F401
