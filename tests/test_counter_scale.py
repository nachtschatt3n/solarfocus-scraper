#!/usr/bin/env python3
"""A counter cannot multiply — dropped decimals are repaired, never confirmed.

Background (2026-08-22 .. 2026-09-03). Four times in 21 days a run-hour meter
was OCR'd with its decimal point swallowed and the 10x/100x value reached Home
Assistant, corrupting the long-term statistics of a `total_increasing` sensor
(daily `change` rows of +59287.8, +125180.0, ... followed by bogus resets):

    2026-08-22  saugaustragung_h   557.4  -> 55751.0   (3 confirmations)
    2026-08-23  saugaustragung_h   557.81 ->  5586.0
    2026-08-27  einschub_h        6594.4  -> 65945.0   (held ~36h)
    2026-09-03  einschub_h        6597.4  -> 65975.0   (self-corrected, 24 min)

Why the existing layers all missed it, in order:

1. parse_value()'s dropped-decimal repair is scoped to exactly-3-digit reads;
   these are 4-6 digits.
2. SANITY_BOUNDS for hour meters is (0, 1_000_000) because they grow without
   limit — so the bounds layer can NEVER fire for them, and the invariant that
   protects every other field ("a bounds failure never advances the delta
   confirmation counter") gave these fields nothing.
3. Monotonicity passes: 10x is an increase.
4. That left the 3-cycle delta_override, which is precisely built to accept a
   stable out-of-budget read — and a repeatedly-misread glyph is stable. The
   breaker promoted the misread to baseline; recovery then needed three MORE
   cycles through _DECREASE_CONFIRM.

The property pinned here is that layers 2-4 no longer stand alone: a value a
power of ten too large is either repaired into a plausible reading, or (failing
that) rejected in a way that can never be confirmed.

Run either way:
    python tests/test_counter_scale.py     (no pytest needed; exits non-zero)
    pytest tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import main as m  # noqa: E402


class _Broker:
    """Minimal stand-in: _sanity_check only calls get_last()."""

    def __init__(self, prev: dict):
        self._prev = {k: str(v) for k, v in prev.items()}

    def get_last(self, field):
        return self._prev.get(field)


def _reset():
    m._LAST_ACCEPTED_TS.clear()
    m._DELTA_CONFIRM.clear()
    m._DECREASE_CONFIRM.clear()


# --------------------------------------------------------------------------
# 1. The four production misreads are repaired to their true readings
# --------------------------------------------------------------------------

# (field, previous published value, misread, expected repair)
PRODUCTION_CASES = [
    ("saugaustragung_h", 557.4, 55751.0, 557.51),
    ("saugaustragung_h", 557.81, 5586.0, 558.6),
    ("einschub_h", 6594.4, 65945.0, 6594.5),
    ("einschub_h", 6597.4, 65975.0, 6597.5),
]


def test_production_misreads_are_repaired():
    for field, prev, misread, expected in PRODUCTION_CASES:
        _reset()
        got = m._counter_scale_repair(field, misread, prev)
        assert got == expected, (
            f"{field} {prev} -> {misread}: expected repair to {expected}, got {got}")


def test_repair_is_applied_to_the_published_values():
    """End-to-end through _sanity_check: the field is ACCEPTED (absent from
    the reject dict) and `values` carries the corrected number, because a
    held-back counter is a gap and we can do better than a gap here."""
    for field, prev, misread, expected in PRODUCTION_CASES:
        _reset()
        values = {field: misread}
        rejected = m._sanity_check(values, _Broker({field: prev}),
                                   allow_delta_override=True)
        assert field not in rejected, f"{field} should be repaired, not rejected"
        assert values[field] == expected, f"{field} not rewritten: {values[field]}"


def test_repair_never_advances_the_confirmation_counter():
    """The corruption path was the breaker promoting a stable misread to
    baseline. A repaired value must leave _DELTA_CONFIRM untouched."""
    _reset()
    for _ in range(m.DELTA_CONFIRM_THRESHOLD + 2):
        values = {"einschub_h": 65945.0}
        m._sanity_check(values, _Broker({"einschub_h": 6594.4}),
                        allow_delta_override=True)
        assert values["einschub_h"] == 6594.5
    assert "einschub_h" not in m._DELTA_CONFIRM


# --------------------------------------------------------------------------
# 2. The ratio guard: what cannot be repaired must not become confirmable
# --------------------------------------------------------------------------

def test_unrepairable_10x_is_never_confirmed():
    """A 10x read whose rescaling does NOT land in budget (here the underlying
    value also moved) must be rejected every cycle, forever — never promoted."""
    _reset()
    prev = 6594.4
    bogus = 98765.0            # /10 = 9876.5, still ~3282 above prev
    for cycle in range(m.DELTA_CONFIRM_THRESHOLD + 3):
        values = {"einschub_h": bogus}
        rejected = m._sanity_check(values, _Broker({"einschub_h": prev}),
                                   allow_delta_override=True)
        assert "einschub_h" in rejected, f"cycle {cycle}: must stay rejected"
        assert "x prev" in rejected["einschub_h"]
    assert "einschub_h" not in m._DELTA_CONFIRM


def test_ratio_guard_stays_out_of_the_way_near_zero():
    """A meter still near zero can legitimately multiply (0.1 -> 0.6 is a real
    0.5h of runtime and a ratio of 6). The guard must not fire below
    COUNTER_RATIO_GUARD_MIN_PREV."""
    _reset()
    values = {"zuendung_h": 0.6}
    rejected = m._sanity_check(values, _Broker({"zuendung_h": 0.1}),
                               allow_delta_override=True)
    assert rejected == {}, rejected
    assert values["zuendung_h"] == 0.6


# --------------------------------------------------------------------------
# 3. Normal operation is UNCHANGED — the repair must not invent corrections
# --------------------------------------------------------------------------

def test_ordinary_increments_are_untouched():
    """Hour meters tick <=0.1/cycle. Nothing about that is a misread."""
    _reset()
    values = {"einschub_h": 6594.5, "saugaustragung_h": 557.5}
    rejected = m._sanity_check(values, _Broker({"einschub_h": 6594.4,
                                                "saugaustragung_h": 557.4}),
                               allow_delta_override=True)
    assert rejected == {}, rejected
    assert values == {"einschub_h": 6594.5, "saugaustragung_h": 557.5}


def test_no_repair_when_value_is_not_a_whole_number():
    """A swallowed '.' always yields an integer. 65945.7 has its decimal point,
    so it is a different (and still rejected) kind of wrong."""
    _reset()
    assert m._counter_scale_repair("einschub_h", 65945.7, 6594.4) is None


def test_no_repair_that_would_move_a_counter_backwards():
    """Rescaling must not manufacture a decrease — that only defers the problem
    to the monotonicity guard."""
    _reset()
    # /10 = 655.0, well below prev; no scale lands >= prev and in budget.
    assert m._counter_scale_repair("einschub_h", 6550.0, 6594.4) is None


def test_catch_up_after_an_outage_is_not_mistaken_for_a_misread():
    """The elapsed-time budget (test_delta_elapsed.py) lets a counter jump
    legitimately after a stall. That is in-budget, so there is nothing to
    repair and nothing to reject."""
    _reset()
    import time as _t
    m._LAST_ACCEPTED_TS["einschub_h"] = _t.time() - 4 * 3600
    assert m._counter_scale_repair("einschub_h", 6597.0, 6594.4) is None
    values = {"einschub_h": 6597.0}
    rejected = m._sanity_check(values, _Broker({"einschub_h": 6594.4}),
                               allow_delta_override=True)
    assert rejected == {}, rejected


def test_non_counter_fields_are_unaffected():
    """Temperatures keep their old behaviour: tight bounds already protect
    them, and 10x a temperature is out of bounds, not repairable."""
    _reset()
    values = {"kesseltemperatur": 620.0}
    rejected = m._sanity_check(values, _Broker({"kesseltemperatur": 62.0}),
                               allow_delta_override=True)
    assert "kesseltemperatur" in rejected
    assert "out of bounds" in rejected["kesseltemperatur"]
    assert values["kesseltemperatur"] == 620.0    # judged, not rewritten


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
