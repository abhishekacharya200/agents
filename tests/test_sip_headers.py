"""Tests for JobContext.get_sip_headers readiness and source selection."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from livekit import rtc
from livekit.agents.job import JobContext

pytestmark = [pytest.mark.unit]


class _FakeRoom(rtc.EventEmitter[str]):
    def __init__(self, participant: Any, rpc_headers: dict[str, str]) -> None:
        super().__init__()
        self.remote_participants = {participant.identity: participant}
        self.rpc_calls = 0

        async def perform_rpc(**_: Any) -> str:
            self.rpc_calls += 1
            return json.dumps({"headers": rpc_headers})

        self.local_participant = SimpleNamespace(perform_rpc=perform_rpc)

    def isconnected(self) -> bool:
        return True


class _SIPParticipant(rtc.RemoteParticipant):
    identity = "caller"  # type: ignore[assignment]
    kind = rtc.ParticipantKind.PARTICIPANT_KIND_SIP  # type: ignore[assignment]
    state = rtc.ParticipantState.PARTICIPANT_STATE_ACTIVE  # type: ignore[assignment]

    def __init__(self, attributes: dict[str, str]) -> None:
        self._attributes = attributes

    @property
    def attributes(self) -> dict[str, str]:
        return self._attributes


def _sip_participant(attributes: dict[str, str]) -> _SIPParticipant:
    return _SIPParticipant(attributes)


def _ctx(room: _FakeRoom) -> Any:
    return SimpleNamespace(_room=room, is_fake_job=lambda: False)


async def test_inbound_all_headers_read_from_attributes() -> None:
    p = _sip_participant(
        {
            "sip.callStatus": "ringing",
            "sip.callIDFull": "abc",
            "sip.h.call-id": "abc",
            "sip.h.x-account": "42",
            "sip.h.via": "SIP/2.0/UDP host",
        }
    )
    room = _FakeRoom(p, rpc_headers={})

    headers = await JobContext.get_sip_headers(_ctx(room), "caller")

    assert headers == {"call-id": "abc", "x-account": "42"}
    assert room.rpc_calls == 0


async def test_unmapped_headers_use_rpc() -> None:
    p = _sip_participant({"sip.callStatus": "ringing", "sip.callIDFull": "abc"})
    room = _FakeRoom(p, rpc_headers={"Call-ID": "abc", "X-Account": "42"})

    headers = await JobContext.get_sip_headers(_ctx(room), "caller")

    assert headers == {"call-id": "abc", "x-account": "42"}
    assert room.rpc_calls == 1


async def test_outbound_waits_for_answer() -> None:
    p = _sip_participant({"sip.callStatus": "dialing"})
    room = _FakeRoom(p, rpc_headers={"Call-ID": "abc"})

    task = asyncio.create_task(JobContext.get_sip_headers(_ctx(room), "caller"))
    await asyncio.sleep(0)

    p.attributes.update({"sip.callStatus": "ringing"})
    room.emit("participant_attributes_changed", ["sip.callStatus"], p)
    await asyncio.sleep(0)
    assert not task.done()

    p.attributes.update({"sip.callIDFull": "abc"})
    room.emit("participant_attributes_changed", ["sip.callIDFull"], p)

    assert await task == {"call-id": "abc"}
    assert room.rpc_calls == 1
