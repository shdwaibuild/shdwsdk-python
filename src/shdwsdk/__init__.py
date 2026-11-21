"""SHDWsdk for Python: non-blocking AI carbon telemetry.

    import shdwsdk
    shdw = shdwsdk.init(api_key="shdw_live_...")          # or SHDW_API_KEY env var
    client = shdwsdk.instrument(OpenAI())                  # or Anthropic()
    with shdwsdk.customer("acme-corp"):                    # per-customer Scope 3
        client.chat.completions.create(model="gpt-5.4-mini", messages=[...])

Energy and carbon are computed server-side from token counts (EcoLogits), so
this package only reports models, token usage and context. It never sends
prompts or completions, and it never raises into your application.
"""

from .client import ShdwClient, get_client, init
from .context import customer, current_customer
from .integrations import instrument

__all__ = [
    "ShdwClient",
    "init",
    "get_client",
    "customer",
    "current_customer",
    "instrument",
]
__version__ = "0.2.0"
