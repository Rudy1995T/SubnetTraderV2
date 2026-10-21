"""``POST /api/v1/manual/*`` (authed + origin-guarded; spec 14 §3.5). Team/owner routes call 11's domain functions
(02 §H3); product/note/flag go through ``manual_service``. URLs stay plain strings (11's ``valid_url`` checks them):
pydantic's ``HttpUrl`` would normalise them (trailing slash) and change the stored evidence.
"""
from __future__ import annotations

from typing import Literal

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.api import hub
from app.api import manual_service as svc
from app.api.common import RuntimeRef
from app.api.errors import ApiError, raise_for_result
from app.integrity import owner_admin, team
from app.util.time import parse_ts

router = APIRouter(prefix="/api/v1/manual")
_ref = RuntimeRef()
configure = _ref.configure
Url = Field(min_length=8, max_length=500)
OptUrl = Field(default=None, max_length=500)


def created(res) -> JSONResponse:
    hub.nudge()
    return JSONResponse(raise_for_result(res), status_code=201)


def done(res) -> dict:
    hub.nudge()
    return raise_for_result(res)


def ts(v: str, name: str) -> int:
    try:
        return parse_ts(v)
    except (TypeError, ValueError):
        raise ApiError("bad_param", 422, f"{name}: not an ISO-8601 time") from None


class TeamAdd(BaseModel):
    netuid: int
    name: str = Field(min_length=1, max_length=120)
    role: str | None = Field(default=None, max_length=60)
    profile_url: str | None = OptUrl
    evidence_url: str | None = OptUrl
    verified: bool = False
    note: str | None = Field(default=None, max_length=1000)


class TeamVerify(BaseModel):
    evidence_urls: list[str] = Field(min_length=1, max_length=5)
    note: str | None = Field(default=None, max_length=1000)


class TeamReject(BaseModel):
    reason: str = Field(min_length=1, max_length=300)
    evidence_url: str | None = OptUrl


class Disclosure(BaseModel):
    netuid: int
    status: Literal["anonymous", "clear"]
    evidence_url: str = Url
    note: str | None = Field(default=None, max_length=1000)


class Product(BaseModel):
    netuid: int
    status: Literal["none", "waitlist", "beta", "live"] | None
    evidence_url: str = Url
    note: str | None = Field(default=None, max_length=1000)


class Note(BaseModel):
    netuid: int
    text: str = Field(min_length=1, max_length=2000)
    evidence_url: str = Url


class Flag(BaseModel):
    netuid: int
    flag: str = Field(min_length=3, max_length=40)
    action: Literal["confirm", "clear"]
    evidence_url: str = Url
    note: str | None = Field(default=None, max_length=1000)


class Whitelist(BaseModel):
    netuid: int
    reason: str
    applies_to: str
    window_start: str
    window_end: str
    evidence_url: str = Url
    change_id: int | None = None
    old_coldkey: str | None = None
    new_coldkey: str | None = None
    note: str | None = Field(default=None, max_length=1000)


class Compromise(BaseModel):
    netuid: int
    window_start: str
    window_end: str
    evidence_url: str = Url
    note: str | None = Field(default=None, max_length=1000)


@router.post("/team")
async def team_add(b: TeamAdd) -> JSONResponse:
    res = await team.add_member(_ref.db(), netuid=b.netuid, name=b.name, role=b.role, profile_url=b.profile_url,
                                evidence_url=b.evidence_url, evidence_note=b.note, source="manual", created_by="api",
                                verify=b.verified, source_ref="api")
    return created(res)


@router.post("/team/{member_id}/verify")
async def team_verify(member_id: int, b: TeamVerify) -> dict:
    return done(await team.verify_member(_ref.db(), member_id, evidence_urls=b.evidence_urls, note=b.note, by="api"))


@router.post("/team/{member_id}/reject")
async def team_reject(member_id: int, b: TeamReject) -> dict:
    return done(await team.reject_member(_ref.db(), member_id, reason=b.reason, evidence_url=b.evidence_url,
                                         by="api"))


@router.post("/team/disclosure")
async def team_disclosure(b: Disclosure) -> JSONResponse:
    return created(await team.set_disclosure(_ref.db(), netuid=b.netuid, status=b.status,
                                             evidence_url=b.evidence_url, note=b.note, by="api"))


@router.post("/product")
async def product(b: Product) -> JSONResponse:
    return created(await svc.set_product(_ref.db(), netuid=b.netuid, status=b.status, evidence_url=b.evidence_url,
                                         note=b.note, by="api"))


@router.post("/note")
async def note(b: Note) -> JSONResponse:
    return created(await svc.add_note(_ref.db(), netuid=b.netuid, text=b.text, evidence_url=b.evidence_url, by="api"))


@router.post("/flag")
async def flag(b: Flag) -> JSONResponse:
    return created(await svc.set_flag(_ref.db(), netuid=b.netuid, flag=b.flag, action=b.action,
                                      evidence_url=b.evidence_url, note=b.note, by="api"))


@router.post("/owner/whitelist")
async def owner_whitelist(b: Whitelist) -> JSONResponse:
    return created(await owner_admin.add_whitelist(
        _ref.db(), netuid=b.netuid, reason=b.reason, applies_to=b.applies_to,
        window_start=ts(b.window_start, "window_start"), window_end=ts(b.window_end, "window_end"),
        evidence_url=b.evidence_url, by="api", change_id=b.change_id, old_coldkey=b.old_coldkey,
        new_coldkey=b.new_coldkey, note=b.note))


@router.post("/owner/whitelist/{whitelist_id}/retire")
async def owner_whitelist_retire(whitelist_id: int) -> dict:
    return done(await owner_admin.retire_whitelist(_ref.db(), whitelist_id, by="api"))


@router.post("/owner/compromise")
async def owner_compromise(b: Compromise) -> JSONResponse:
    return created(await owner_admin.override_key_compromise(
        _ref.db(), netuid=b.netuid, window_start=ts(b.window_start, "window_start"),
        window_end=ts(b.window_end, "window_end"), evidence_url=b.evidence_url, by="api", note=b.note))
