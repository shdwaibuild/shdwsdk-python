import json

import pytest

from shdwsdk import ShdwClient
from shdwsdk.client import SDK_VERSION


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class Sender:
    def __init__(self, *statuses):
        self.statuses = list(statuses)
        self.bodies = []

    def __call__(self, url, headers, body, timeout):
        self.bodies.append(json.loads(body))
        return self.statuses.pop(0) if len(self.statuses) > 1 else self.statuses[0]


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr("shdwsdk.client.time.sleep", lambda s: None)


def make(*statuses, **kw):
    sender, clock = Sender(*statuses), Clock()
    client = ShdwClient("shdw_live_test_key_123", sender=sender, start_thread=False, clock=clock, **kw)
    return client, sender, clock


def test_drop_is_reported_with_next_accepted_batch():
    shdw, sender, _ = make(401, 202)
    shdw.record_usage("gpt-5.5", 10, 5)
    shdw.flush()  # rejected
    shdw.record_usage("gpt-5.5", 10, 5)
    shdw.flush()  # accepted with report
    assert "delivery" not in sender.bodies[0]
    assert sender.bodies[1]["delivery"] == {
        "sdk": "sdk-python",
        "sdk_version": SDK_VERSION,
        "delivered": 0,
        "dropped": 1,
        "retried": 0,
        "drop_reasons": {"rejected_auth": 1},
        "pending": 0,
    }
    shdw.record_usage("gpt-5.5", 10, 5)
    shdw.flush()
    assert "delivery" not in sender.bodies[2]


def test_unreachable_counts_retry_and_drop():
    shdw, sender, clock = make(503, 503, 202)
    shdw.record_usage("gpt-5.5", 10, 5)
    shdw.flush()  # two attempts, then dropped
    assert shdw.stats["retried"] == 1 and shdw.stats["dropped"] == 1
    shdw.record_usage("gpt-5.5", 10, 5)
    shdw.flush()
    assert sender.bodies[-1]["delivery"]["drop_reasons"] == {"unreachable": 1}
    assert sender.bodies[-1]["delivery"]["retried"] == 1


def test_idle_report_after_five_minutes_only_when_needed():
    shdw, sender, clock = make(202)
    shdw.flush()
    assert sender.bodies == []
    shdw.record_usage("gpt-5.5", 10, 5)
    shdw.flush()
    clock.t += 120
    shdw.flush()
    assert len(sender.bodies) == 1
    clock.t += 200
    shdw.flush()
    assert sender.bodies[1]["events"] == []
    assert sender.bodies[1]["delivery"]["delivered"] == 1
    clock.t += 400
    shdw.flush()
    assert len(sender.bodies) == 2


def test_delivery_reports_can_be_disabled(monkeypatch):
    shdw, sender, clock = make(401, 202, delivery_reports=False)
    shdw.record_usage("gpt-5.5", 10, 5)
    shdw.flush()
    clock.t += 600
    shdw.record_usage("gpt-5.5", 10, 5)
    shdw.flush()
    shdw.flush()
    assert all("delivery" not in b for b in sender.bodies)

    monkeypatch.setenv("SHDW_DELIVERY_REPORTS", "0")
    assert ShdwClient("shdw_live_test_key_123", start_thread=False).delivery_reports is False
