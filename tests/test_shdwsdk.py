import asyncio
import json
from datetime import datetime, timedelta
from types import SimpleNamespace as NS

import shdwsdk
from shdwsdk import ShdwClient
from shdwsdk.integrations.litellm import ShdwLiteLLMLogger


class Recorder:
    def __init__(self, status=202):
        self.calls = []
        self.status = status

    def __call__(self, url, headers, body, timeout):
        self.calls.append((url, headers, json.loads(body), timeout))
        return self.status

    @property
    def events(self):
        return [e for _, _, b, _ in self.calls for e in b["events"]]


def make(status=202):
    rec = Recorder(status)
    return ShdwClient("shdw_live_test_key_123", sender=rec, start_thread=False), rec


def test_record_and_flush_with_customer_context():
    shdw, rec = make()
    with shdwsdk.customer("acme-corp"):
        shdw.record_usage("claude-sonnet-5-5", 100, 20)
    shdw.record_usage("gpt-5.4-mini", 10, 5, customer_ref="jane@acme.com")  # email rejected
    shdw.flush()
    url, headers, _, timeout = rec.calls[0]
    assert headers["X-SHDW-Source"] == "sdk-python" and headers["X-SHDW-Key"].startswith("shdw_")
    assert timeout <= 1.5
    assert [e.get("customer_ref") for e in rec.events] == ["acme-corp", None]
    assert [e["provider"] for e in rec.events] == ["anthropic", "openai"]
    assert shdw.stats == {"queued": 2, "delivered": 2, "dropped": 0}
    assert all("grid_region" not in e for e in rec.events)  # server applies the Settings region


def test_explicit_grid_region_is_sent():
    rec = Recorder()
    shdw = ShdwClient("shdw_live_test_key_123", grid_region="eu-north-1", sender=rec, start_thread=False)
    shdw.record_usage("gpt-5.4-mini", 1, 1)
    shdw.flush()
    assert rec.events[0]["grid_region"] == "eu-north-1"


def test_never_raises_and_drops_on_4xx():
    def boom(*a):
        raise OSError("network down")

    shdw = ShdwClient("shdw_live_test_key_123", sender=boom, start_thread=False)
    shdw.record_usage("gpt-5.5", 1, 1)
    shdw.flush()
    assert shdw.stats["dropped"] == 1

    shdw, rec = make(status=401)
    shdw.record_usage("gpt-5.5", 1, 1)
    shdw.flush()
    assert len(rec.calls) == 1 and shdw.stats["dropped"] == 1  # no retry on auth errors


def _fake_openai(result):
    completions = NS(create=lambda **kw: result(kw))
    return NS(chat=NS(completions=completions))


def test_instrument_openai_sync_and_stream():
    shdw, rec = make()
    resp = NS(model="gpt-5.4-mini", usage=NS(prompt_tokens=42, completion_tokens=7))

    def result(kw):
        if kw.get("stream"):
            return iter([NS(model="gpt-5.4-mini", usage=None), NS(model="gpt-5.4-mini", usage=NS(prompt_tokens=5, completion_tokens=9))])
        return resp

    client = shdwsdk.instrument(_fake_openai(result), shdw=shdw)
    assert client.chat.completions.create(model="gpt-5.4-mini", messages=[]) is resp
    with shdwsdk.customer("globex"):
        chunks = list(client.chat.completions.create(model="gpt-5.4-mini", messages=[], stream=True))
    assert len(chunks) == 2
    shdw.flush()
    assert [(e["prompt_tokens"], e["completion_tokens"], e["is_streaming"]) for e in rec.events] == [(42, 7, False), (5, 9, True)]
    assert rec.events[1]["customer_ref"] == "globex"
    # idempotent
    shdwsdk.instrument(client, shdw=shdw)
    assert client.chat.completions.create.__shdw_wrapped__


def test_instrument_async_anthropic():
    shdw, rec = make()

    class Messages:
        async def create(self, **kw):
            return {"model": "claude-haiku-5-5", "usage": {"input_tokens": 11, "output_tokens": 3}}

    client = shdwsdk.instrument(NS(messages=Messages()), shdw=shdw)
    asyncio.run(client.messages.create(model="claude-haiku-5-5", max_tokens=10, messages=[]))
    shdw.flush()
    assert rec.events[0]["provider"] == "anthropic"
    assert (rec.events[0]["prompt_tokens"], rec.events[0]["completion_tokens"]) == (11, 3)


def test_litellm_logger():
    shdw, rec = make()
    logger = ShdwLiteLLMLogger(shdw)
    start = datetime(2026, 11, 19, 12, 0, 0)
    logger.log_success_event(
        {"model": "claude-sonnet-5-5", "litellm_params": {"custom_llm_provider": "anthropic", "metadata": {"shdw_customer_ref": "acme-corp"}}},
        NS(model="claude-sonnet-5-5", usage=NS(prompt_tokens=300, completion_tokens=80)),
        start,
        start + timedelta(milliseconds=850),
    )
    shdw.flush()
    e = rec.events[0]
    assert e["ingest_source"] == "litellm" and e["customer_ref"] == "acme-corp"
    assert e["provider"] == "anthropic" and e["latency_ms"] == 850
