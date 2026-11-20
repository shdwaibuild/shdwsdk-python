"""Wrap provider clients so every call reports token usage."""

from __future__ import annotations

from typing import Any, Optional

from ..client import ShdwClient
from ._wrap import instrument_client


def instrument(client: Any, *, shdw: Optional[ShdwClient] = None, provider: Optional[str] = None) -> Any:
    """Instrument an ``openai.OpenAI``/``AsyncOpenAI`` or ``anthropic.Anthropic``/``AsyncAnthropic`` client.

    Patches the client instance in place and returns it. Responses are returned
    unchanged; streaming responses are passed through chunk by chunk.
    """
    return instrument_client(client, shdw=shdw, provider=provider)


__all__ = ["instrument"]
