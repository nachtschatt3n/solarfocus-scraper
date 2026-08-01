# CLAUDE.md

> **Dedicated agent — prefer it for non-trivial work.**
> This repo is owned by the global **`solarfocus-agent`**
> (`~/.claude/agents/solarfocus-agent.md`) — the Solarfocus pellet^top
> pellet-heater specialist. It carries the deep, cross-session context for
> this codebase that isn't obvious from the source:
>
> - the **VNC → OCR → MQTT / Home Assistant** cycle and the `main.py` layout;
> - the **data-quality framework** — the per-cycle sanity pipeline
>   (bounds → monotonicity → delta-breaker) and the invariant that a **bounds
>   failure never advances the delta-confirmation counter**;
> - **`scraper_last_run`** as the only reliable liveness signal (HA drops
>   state-equal MQTT updates), and the **idle/power-save "known-NORMAL"**
>   pattern (don't flag stable values as regressions);
> - the **OCR pipeline** (unit-glyph bbox exclusion, LCD/Otsu), how to **test**
>   an OCR/sanity change, and the retained-MQTT baseline wipe order;
> - the **build/deploy pipeline** (push → GHCR image → cberg-agent bumps the
>   Flux `image.tag`), and the **cross-agent seam**: cluster/Flux/ops →
>   **cberg-agent**, Home Assistant → **ha-agent**.
>
> Invoke it explicitly (`use solarfocus-agent to …`) or let it auto-route on
> heater / scraper / data-quality tasks.

Repo architecture, sensors, and run/deploy details: see [`README.md`](./README.md).
Sibling system (fuel pricing / buy-signal): `pellet-price-monitor` — same agent owns it.
