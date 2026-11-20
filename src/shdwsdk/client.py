"""Background, fail-silent telemetry queue."""

from __future__ import annotations

import atexit
import json
import logging
import os
import threading
import time
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from .context import current_customer, valid_customer_ref

log = logging.getLogger("shdwsdk")

DEFAULT_ENDPOINT = "https://www.shdwsdk.ai/api/v1/ingest-telemetry"
MAX_TIMEOUT_SECONDS = 1.5
MAX_BUFFER = 5000
MAX_BATCH = 250
SDK_VERSION = "0.1.2"

Sender = Callable[[str, Dict[str, str], bytes, float], int]


def _urllib_send(url: str, headers: Dict[str, str], body: bytes, timeout: float) -> int:
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (fixed https endpoint)
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code


def _infer_provider(model: str) -> str:
    m = model.lower()
    if "claude" in m:
        return "anthropic"
    if "gemini" in m or "gemma" in m:
        return "gemini"
    if "gpt" in m or m.startswith(("o1", "o3", "o4")):
        return "openai"
    return "custom"


class ShdwClient:
    """Queues usage events and delivers them from a daemon thread.

    Never raises into the host application. Network calls time out after
    at most 1.5 s. Use ``flush()`` before short-lived processes exit.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        *,
        endpoint: Optional[str] = None,
        grid_region: Optional[str] = None,
        workload_name: Optional[str] = None,
        batch_size: int = 50,
        flush_interval: float = 5.0,
        timeout: float = MAX_TIMEOUT_SECONDS,
        source: str = "sdk-python",
        sender: Optional[Sender] = None,
        start_thread: bool = True,
    ) -> None:
        self.api_key = api_key or os.environ.get("SHDW_API_KEY", "")
        self.endpoint = endpoint or os.environ.get("SHDW_ENDPOINT", DEFAULT_ENDPOINT)
        # None → omitted, so the server applies the region chosen in SHDWsdk Settings.
        self.grid_region = grid_region or os.environ.get("SHDW_GRID_REGION") or None
        self.workload_name = workload_name or os.environ.get("SHDW_WORKLOAD_NAME")
        self.batch_size = max(1, min(batch_size, MAX_BATCH))
        self.flush_interval = max(0.5, flush_interval)
        self.timeout = min(max(0.1, timeout), MAX_TIMEOUT_SECONDS)
        self.source = source
        self._send = sender or _urllib_send
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._closed = False
        self.stats = {"queued": 0, "delivered": 0, "dropped": 0}
        self._thread: Optional[threading.Thread] = None
        if start_thread:
            self._thread = threading.Thread(target=self._run, name="shdwsdk", daemon=True)
            self._thread.start()
            atexit.register(self.close)

    # ------------------------------------------------------------------ public
    def record_usage(
        self,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        *,
        provider: Optional[str] = None,
        latency_ms: Optional[float] = None,
        is_streaming: bool = False,
        customer_ref: Optional[str] = None,
        grid_region: Optional[str] = None,
        workload_name: Optional[str] = None,
        ingest_source: Optional[str] = None,
    ) -> None:
        try:
            if not self.api_key or self._closed:
                return
            model = str(model or "unknown-model")[:128]
            event: Dict[str, Any] = {
                "model_name": model,
                "provider": provider or _infer_provider(model),
                "prompt_tokens": max(0, int(prompt_tokens or 0)),
                "completion_tokens": max(0, int(completion_tokens or 0)),
                "is_streaming": bool(is_streaming),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            region = grid_region or self.grid_region
            if region:
                event["grid_region"] = region
            if latency_ms is not None and latency_ms > 0:
                event["latency_ms"] = int(latency_ms)
            ref = valid_customer_ref(customer_ref) or current_customer()
            if ref:
                event["customer_ref"] = ref
            name = workload_name or self.workload_name
            if name:
                event["workload_name"] = name
            if ingest_source:
                event["ingest_source"] = ingest_source
            with self._lock:
                if len(self._buffer) >= MAX_BUFFER:
                    self._buffer.pop(0)
                    self.stats["dropped"] += 1
                self._buffer.append(event)
                self.stats["queued"] += 1
                full = len(self._buffer) >= self.batch_size
            if full:
                self._wake.set()
        except Exception:  # pragma: no cover - fail-silent by contract
            log.debug("shdwsdk: failed to record usage", exc_info=True)

    def flush(self) -> None:
        """Deliver everything queued now (blocks for at most a few timeouts)."""
        try:
            while True:
                with self._lock:
                    batch = self._buffer[: MAX_BATCH]
                    del self._buffer[: MAX_BATCH]
                if not batch:
                    return
                self._deliver(batch)
        except Exception:  # pragma: no cover
            log.debug("shdwsdk: flush failed", exc_info=True)

    def close(self) -> None:
        if self._closed:
            return
        self.flush()
        self._closed = True
        self._wake.set()

    # ---------------------------------------------------------------- internal
    def _deliver(self, batch: List[Dict[str, Any]]) -> None:
        body = json.dumps({"events": batch}).encode()
        headers = {
            "Content-Type": "application/json",
            "X-SHDW-Key": self.api_key,
            "X-SHDW-Source": self.source,
            "X-SHDW-Python-SDK-Version": SDK_VERSION,
        }
        for attempt in range(2):
            try:
                status = self._send(self.endpoint, headers, body, self.timeout)
            except Exception:
                status = 0
            if 200 <= status < 300:
                self.stats["delivered"] += len(batch)
                return
            if 400 <= status < 500 and status != 429:
                break  # bad key or payload: retrying will not help
            time.sleep(0.2 * (attempt + 1))
        self.stats["dropped"] += len(batch)
        log.warning("shdwsdk: dropped %d telemetry events (HTTP %s)", len(batch), status)

    def _run(self) -> None:
        while not self._closed:
            self._wake.wait(self.flush_interval)
            self._wake.clear()
            self.flush()


_default: Optional[ShdwClient] = None


def init(api_key: Optional[str] = None, **kwargs: Any) -> ShdwClient:
    """Create (or replace) the process-wide client used by integrations."""
    global _default
    if _default is not None:
        _default.close()
    _default = ShdwClient(api_key, **kwargs)
    return _default


def get_client() -> Optional[ShdwClient]:
    global _default
    if _default is None and os.environ.get("SHDW_API_KEY"):
        _default = ShdwClient()
    return _default
