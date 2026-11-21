# Changelog

## 0.2.0 (2026-11-21)

- **Delivery reports.** The client sends its delivery counters (delivered, dropped by reason, retried, queue backlog) with normal batches about once a minute, or on their own every five minutes when idle. They power drop rates in the SHDWsdk developer console. Counters only. Opt out with `delivery_reports=False` or `SHDW_DELIVERY_REPORTS=0`.
- `client.stats` now includes `retried`.

## 0.1.2 (2026-11-20)

- The PyPI page now shows the new README and links to this repository (source, issues, docs).
- No runtime changes.

## 0.1.1

- Grid region is no longer defaulted; events without one use your organization's default region setting.
