"""Request-scoped customer attribution (works with threads and asyncio)."""

from __future__ import annotations

import contextvars
import re
from contextlib import contextmanager
from typing import Iterator, Optional

_current: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar("shdw_customer_ref", default=None)

# Pseudonymous account ids only. Emails are rejected by the server as well.
CUSTOMER_REF_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")


def valid_customer_ref(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = str(value).strip()
    return value if CUSTOMER_REF_PATTERN.match(value) else None


@contextmanager
def customer(customer_ref: str) -> Iterator[None]:
    """Attribute every AI call inside this block to one of your customers."""
    token = _current.set(valid_customer_ref(customer_ref))
    try:
        yield
    finally:
        _current.reset(token)


def current_customer() -> Optional[str]:
    return _current.get()
