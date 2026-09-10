#!/usr/bin/env python3
"""The fuel counter gets the bounds protection the run-hour meters cannot have.

`pelletsverbrauch_kg` is the number everything downstream rests on: the HA
utility meters, the Energy dashboard, the per-day/month averages, and
ultimately
the "should I buy pellets" question. It is also a counter, and counters were
the one family where the sanity pipeline had no backstop.

The run-hour meters are bounded (0, 1_000_000) because they grow without limit,
so their bounds layer can never fire — which means the invariant that protects
every other field ("a bounds failure never advances the delta confirmation
counter") gives them nothing at all. That is precisely the hole that let
einschub_h round-trip 10x four times in 21 days and corrupt Home Assistant's
long-term statistics badly enough to need a 241-adjustment recorder repair on
2026-09-08.

pelletsverbrauch_kg does not need the wide range: it is a lifetime fuel total
around 16_350 kg rising ~4_000 kg/year. Narrowing it to (10_000, 100_000)
restores the backstop for the one counter where a confirmed misread would be
expensive rather than merely wrong.

This field has never actually misread. The tests below are the guard, not the
cure.

Run either way:
    python tests/test_fuel_counter_bounds.py     (no pytest needed)
    pytest tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import main as m  # noqa: E402

FIELD = "pelletsverbrauch_kg"
LO, HI = m.SANITY_BOUNDS[FIELD]

# Roughly where the real counter sits, and where it will sit for years.
LIVE = 16_348.0


class _Broker:
    def __init__(self, prev):
        self._prev = {k: str(v) for k, v in prev.items()}

    def get_last(self, field):
        return self._prev.get(field)


def _reset():
    m._LAST_ACCEPTED_TS.clear()
    m._DELTA_CONFIRM.clear()
    m._DECREASE_CONFIRM.clear()


def test_the_live_counter_is_comfortably_inside_the_bounds():
    """A bound that the real value could drift into is a bound that will one
    day reject good data."""
    assert LO < LIVE < HI
    assert LIVE - LO > 5_000, "lower bound too close to the live value"
    # ~4_000 kg/year, so the ceiling should be many years away.
    assert (HI - LIVE) / 4_000 > 15, "upper bound too close for comfort"


def test_a_dropped_leading_digit_is_rejected_by_bounds():
    """16_348 -> 6_348. Under the old (0, 10_000_000) bounds this was in range,
    so it reached the 3-cycle delta override — the mechanism that confirmed the
    einschub_h corruption into the baseline."""
    _reset()
    rejected = m._sanity_check({FIELD: 6_348.0}, _Broker({FIELD: LIVE}),
                               allow_delta_override=True)
    assert FIELD in rejected
    assert "out of bounds" in rejected[FIELD]


def test_a_swallowed_decimal_is_rejected_by_bounds():
    """16_348 -> 1_634.8."""
    _reset()
    rejected = m._sanity_check({FIELD: 1_634.8}, _Broker({FIELD: LIVE}),
                               allow_delta_override=True)
    assert FIELD in rejected
    assert "out of bounds" in rejected[FIELD]


def test_an_added_digit_is_rejected_by_bounds():
    """16_348 -> 163_480, the inflating direction."""
    _reset()
    rejected = m._sanity_check({FIELD: 163_480.0}, _Broker({FIELD: LIVE}),
                               allow_delta_override=True)
    assert FIELD in rejected
    assert "out of bounds" in rejected[FIELD]


def test_an_out_of_bounds_read_can_never_be_confirmed():
    """The whole point. A bounds failure must not advance _DELTA_CONFIRM, so a
    persistent misread cannot be promoted to baseline no matter how stable."""
    _reset()
    broker = _Broker({FIELD: LIVE})
    for _ in range(m.DELTA_CONFIRM_THRESHOLD + 3):
        rejected = m._sanity_check({FIELD: 6_348.0}, broker,
                                   allow_delta_override=True)
        assert FIELD in rejected, "must stay rejected forever"
    assert FIELD not in m._DELTA_CONFIRM
    assert FIELD not in m._DECREASE_CONFIRM


def test_normal_consumption_still_passes():
    """A real burn is a few kg per cycle; nothing about this may be rejected."""
    _reset()
    for step in (0.0, 1.0, 5.0, 19.0):
        _reset()
        rejected = m._sanity_check({FIELD: LIVE + step}, _Broker({FIELD: LIVE}),
                                   allow_delta_override=True)
        assert rejected == {}, f"+{step} kg wrongly rejected: {rejected}"


def test_the_delta_gate_still_guards_in_range_jumps():
    """Bounds are a backstop, not a replacement. A physically impossible jump
    that is still inside the range must remain the delta gate's job."""
    _reset()
    rejected = m._sanity_check({FIELD: LIVE + 500.0}, _Broker({FIELD: LIVE}),
                               allow_delta_override=True)
    assert FIELD in rejected
    assert "exceeds" in rejected[FIELD]


def test_the_run_hour_meters_deliberately_keep_their_wide_bounds():
    """They genuinely grow without limit, which is why they needed the separate
    magnitude guard instead. Narrowing them is not the fix."""
    for f in ("einschub_h", "saugaustragung_h", "pelletsbetrieb_h"):
        lo, hi = m.SANITY_BOUNDS[f]
        assert (lo, hi) == (0, 1_000_000), f"{f} bounds changed: {(lo, hi)}"


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
