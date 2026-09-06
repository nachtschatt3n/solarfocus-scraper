#!/usr/bin/env python3
"""Every cycle closes with exactly one terminal log line.

Background (2026-09-06 10:23:49). A concurrent VNC session made one cycle fail
with `navigation_failed`. The metric incremented correctly, but the last log
line was the `navigate_stuck_screen` ERROR — the cycle never said it had ended.
Triaging from logs alone showed a `cycle_start` that simply stopped, so telling
"failed and will retry" from "hung" meant reconciling against Prometheus. For
several minutes a clean fail-and-retry read as a hang.

Only the success path emitted `cycle_complete`. Every other terminal outcome
(navigation_failed, sanity_failed, busy, error, paused, maintenance) returned
without one.

Pinned here: a closing line exists for the non-success outcomes, it is never
emitted alongside `cycle_complete`, and its severity distinguishes an
operator-initiated idle from a genuine failure — a paused scraper must not read
as broken.

Run either way:
    python tests/test_cycle_terminal_log.py     (no pytest needed)
    pytest tests/
"""
from __future__ import annotations

import logging
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import main as m  # noqa: E402

FAILURES = ("navigation_failed", "sanity_failed", "busy", "error")


class _Spy:
    """Capture event() calls without emitting them."""

    def __enter__(self):
        self.calls: list[tuple[int, str, dict]] = []
        self._orig = m.event
        m.event = lambda level, name, msg, **kw: self.calls.append((level, name, kw))
        return self

    def __exit__(self, *exc):
        m.event = self._orig
        return False


def test_failures_close_at_error_level():
    """A failure the operator did not ask for must not hide at INFO."""
    for status in FAILURES:
        with _Spy() as spy:
            m._log_cycle_terminal(status, "boom", time.time() - 1.0)
        assert len(spy.calls) == 1, f"{status}: {spy.calls}"
        level, name, kw = spy.calls[0]
        assert name == "cycle_end", status
        assert level == logging.ERROR, f"{status} closed at {level}"
        assert kw["status"] == status
        assert kw["error"] == "boom"


def test_operator_idles_are_not_errors():
    """Pausing the scraper yourself is not a fault and must not page anyone."""
    for status in m.CYCLE_OPERATOR_IDLE_STATUSES:
        with _Spy() as spy:
            m._log_cycle_terminal(status, None, time.time())
        level, name, _ = spy.calls[0]
        assert name == "cycle_end"
        assert level == logging.INFO, f"{status} closed at {level}"


def test_the_closing_line_carries_a_duration():
    """Without it the line cannot answer 'did it fail fast or hang first?'."""
    with _Spy() as spy:
        m._log_cycle_terminal("navigation_failed", None, time.time() - 12.0)
    kw = spy.calls[0][2]
    assert kw["duration_s"] >= 12.0


def test_success_is_not_double_logged():
    """cycle_complete is already the success path's closing line; the finally
    block must not add a second one."""
    assert "cycle_complete" not in m.CYCLE_OPERATOR_IDLE_STATUSES
    src = (ROOT / "main.py").read_text(encoding="utf-8")
    # The guard flag is what prevents the double log — if it ever disappears,
    # this test should fail loudly rather than silently allow two lines.
    assert "terminal_logged = True" in src
    assert "if not terminal_logged:" in src


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    print("ALL PASS" if not failures else f"{failures} FAILED")
    raise SystemExit(1 if failures else 0)
