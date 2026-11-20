from __future__ import annotations

import functools
import inspect
import time
from typing import Any, Callable, Iterable, Optional, Tuple

from ..client import ShdwClient, get_client
from ..context import current_customer

# (path to resource, method name, provider)
_TARGETS = [
    (("chat", "completions"), "create", "openai"),
    (("responses",), "create", "openai"),
    (("messages",), "create", "anthropic"),
]


def _get(obj: Any, key: str) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def usage_from_response(resp: Any) -> Tuple[Optional[str], int, int]:
    """Returns (model, prompt_tokens, completion_tokens) from OpenAI/Anthropic objects or dicts."""
    usage = _get(resp, "usage")
    model = _get(resp, "model")
    prompt = _get(usage, "prompt_tokens") or _get(usage, "input_tokens") or 0
    completion = _get(usage, "completion_tokens") or _get(usage, "output_tokens") or 0
    if not prompt and not completion:
        # Anthropic streaming events carry usage on message_start / message_delta
        message = _get(resp, "message")
        if message is not None:
            return usage_from_response(message)
    return (str(model) if model else None), int(prompt or 0), int(completion or 0)


class _StreamTally:
    def __init__(self) -> None:
        self.model: Optional[str] = None
        self.prompt = 0
        self.completion = 0

    def add(self, chunk: Any) -> None:
        model, p, c = usage_from_response(chunk)
        self.model = self.model or model
        self.prompt = max(self.prompt, p)
        self.completion = max(self.completion, c)


def _report(shdw: Optional[ShdwClient], provider: str, model: Optional[str], p: int, c: int,
            started: float, streaming: bool, customer: Optional[str], fallback_model: Optional[str]) -> None:
    client = shdw or get_client()
    if client is None:
        return
    client.record_usage(
        model or fallback_model or "unknown-model",
        p,
        c,
        provider=provider,
        latency_ms=(time.monotonic() - started) * 1000,
        is_streaming=streaming,
        customer_ref=customer,
    )


def _wrap_sync_stream(stream: Iterable[Any], done: Callable[[_StreamTally], None]) -> Any:
    tally = _StreamTally()

    class Wrapped:
        def __iter__(self):
            try:
                for chunk in stream:
                    try:
                        tally.add(chunk)
                    except Exception:
                        pass
                    yield chunk
            finally:
                done(tally)

        def __getattr__(self, name: str) -> Any:
            return getattr(stream, name)

        def __enter__(self):
            return self

        def __exit__(self, *exc: Any) -> None:
            close = getattr(stream, "close", None)
            if callable(close):
                close()

    return Wrapped()


def _wrap_async_stream(stream: Any, done: Callable[[_StreamTally], None]) -> Any:
    tally = _StreamTally()

    class Wrapped:
        async def __aiter__(self):
            try:
                async for chunk in stream:
                    try:
                        tally.add(chunk)
                    except Exception:
                        pass
                    yield chunk
            finally:
                done(tally)

        def __getattr__(self, name: str) -> Any:
            return getattr(stream, name)

    return Wrapped()


def _patch(resource: Any, method: str, provider: str, shdw: Optional[ShdwClient]) -> None:
    original = getattr(resource, method, None)
    if original is None or getattr(original, "__shdw_wrapped__", False):
        return

    if inspect.iscoroutinefunction(original):
        @functools.wraps(original)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            started, customer, model_hint = time.monotonic(), current_customer(), kwargs.get("model")
            result = await original(*args, **kwargs)
            try:
                if kwargs.get("stream"):
                    return _wrap_async_stream(result, lambda t: _report(
                        shdw, provider, t.model, t.prompt, t.completion, started, True, customer, model_hint))
                _report(shdw, provider, *usage_from_response(result), started, False, customer, model_hint)
            except Exception:
                pass
            return result

        async_wrapper.__shdw_wrapped__ = True  # type: ignore[attr-defined]
        setattr(resource, method, async_wrapper)
        return

    @functools.wraps(original)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        started, customer, model_hint = time.monotonic(), current_customer(), kwargs.get("model")
        result = original(*args, **kwargs)
        try:
            if kwargs.get("stream"):
                return _wrap_sync_stream(result, lambda t: _report(
                    shdw, provider, t.model, t.prompt, t.completion, started, True, customer, model_hint))
            _report(shdw, provider, *usage_from_response(result), started, False, customer, model_hint)
        except Exception:
            pass
        return result

    wrapper.__shdw_wrapped__ = True  # type: ignore[attr-defined]
    setattr(resource, method, wrapper)


def instrument_client(client: Any, *, shdw: Optional[ShdwClient] = None, provider: Optional[str] = None) -> Any:
    for path, method, default_provider in _TARGETS:
        resource = client
        for attr in path:
            resource = getattr(resource, attr, None)
            if resource is None:
                break
        if resource is not None:
            _patch(resource, method, provider or default_provider, shdw)
    return client
