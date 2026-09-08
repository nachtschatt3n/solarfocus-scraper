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
> - the **OCR pipeline** (unit-glyph bbox exclusion, LCD/Otsu), the
>   **screenshot-fixture regression suite** in `tests/` (run
>   `python tests/test_screen_recognition.py` or `pytest tests/`; needs
>   tesseract `deu`, **Python 3.12 and Pillow 12.3.0** — the pixel-level tests
>   raise `AttributeError: get_flattened_data` on Pillow 11, and tesseract
>   5.3.x decodes small digit glyphs differently from the pinned 5.5.0, so a
>   venv that drifts from the image tests something other than production; CI
>   now runs the suite *inside* the built image for exactly this reason) plus
>   how to verify OCR/sanity changes by observation, and
>   the retained-MQTT baseline wipe order;
> - the **build/deploy pipeline** (push → GHCR image → cberg-agent bumps the
>   Flux `image.tag`), and the **cross-agent seam**: cluster/Flux/ops →
>   **cberg-agent**, Home Assistant → **ha-agent**.
>
> Invoke it explicitly (`use solarfocus-agent to …`) or let it auto-route on
> heater / scraper / data-quality tasks.

Repo architecture, sensors, and run/deploy details: see [`README.md`](./README.md).
Sibling system (fuel pricing / buy-signal): `pellet-price-monitor` — same agent owns it.
