"""LiteLLM callback: one line covers every provider LiteLLM routes to.

SDK:
    import litellm
    from shdwsdk.integrations.litellm import ShdwLiteLLMLogger
    litellm.callbacks = [ShdwLiteLLMLogger()]

Proxy (config.yaml):
    litellm_settings:
      callbacks: shdwsdk.integrations.litellm.proxy_handler_instance

Per-customer attribution: pass ``metadata={"shdw_customer_ref": "acme-corp"}``
on a request, or use ``shdwsdk.customer(...)``. On the proxy, the virtual key's
``user_api_key_team_alias`` / ``end_user`` are NOT used automatically because
they may contain emails; map them yourself in metadata if they are account ids.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from ..client import ShdwClient, get_client
from ..context import current_customer

try:  # LiteLLM is optional
    from litellm.integrations.custom_logger import CustomLogger  # type: ignore
except Exception:  # pragma: no cover - exercised only without litellm installed
    class CustomLogger:  # type: ignore[no-redef]
        pass


def _provider(kwargs: Dict[str, Any]) -> Optional[str]:
    name = str((kwargs.get("litellm_params") or {}).get("custom_llm_provider") or kwargs.get("custom_llm_provider") or "")
    name = name.lower()
    if "anthropic" in name:
        return "anthropic"
    if "gemini" in name or "vertex" in name:
        return "gemini"
    if "openai" in name or "azure" in name:
        return "openai"
    return None


class ShdwLiteLLMLogger(CustomLogger):  # type: ignore[misc]
    def __init__(self, shdw: Optional[ShdwClient] = None) -> None:
        super().__init__()
        self._shdw = shdw

    def _record(self, kwargs: Dict[str, Any], response_obj: Any, start_time: Any, end_time: Any) -> None:
        try:
            client = self._shdw or get_client()
            if client is None:
                return
            usage = getattr(response_obj, "usage", None)
            if usage is None and isinstance(response_obj, dict):
                usage = response_obj.get("usage")
            get = (lambda k: usage.get(k)) if isinstance(usage, dict) else (lambda k: getattr(usage, k, None))
            prompt = int(get("prompt_tokens") or 0) if usage is not None else 0
            completion = int(get("completion_tokens") or 0) if usage is not None else 0
            model = getattr(response_obj, "model", None) or kwargs.get("model") or "unknown-model"
            metadata = (kwargs.get("litellm_params") or {}).get("metadata") or kwargs.get("metadata") or {}
            latency_ms = None
            try:
                latency_ms = (end_time - start_time).total_seconds() * 1000
            except Exception:
                pass
            client.record_usage(
                str(model),
                prompt,
                completion,
                provider=_provider(kwargs),
                latency_ms=latency_ms,
                is_streaming=bool(kwargs.get("stream")),
                customer_ref=metadata.get("shdw_customer_ref") or current_customer(),
                ingest_source="litellm",
            )
        except Exception:
            pass

    def log_success_event(self, kwargs, response_obj, start_time, end_time):  # noqa: D401
        self._record(kwargs, response_obj, start_time, end_time)

    async def async_log_success_event(self, kwargs, response_obj, start_time, end_time):
        self._record(kwargs, response_obj, start_time, end_time)


proxy_handler_instance = ShdwLiteLLMLogger()
