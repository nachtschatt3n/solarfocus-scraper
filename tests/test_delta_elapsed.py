#!/usr/bin/env python3
"""The delta gate ages its budget with elapsed time — without going blind.

Background (2026-08-31). MAX_DELTA_PER_CYCLE is a per-CYCLE budget, but the gap
between two readings is not constant. A 6.4h stall was followed by a 12-field
rejection wave on resume: kesseltemperatur 38->69, outside_temperature 26->16,
four run-hour counters, puffer_temp_top 40->71. Every one of those readings was
physically correct; they were rejected only because the gate compared them
against a 6.4-hour-old baseline using a 5-minute budget. The control group is
the roll ~2h later with a ~2 minute gap: three clean 44/0 cycles, no wave.

Two properties are pinned here, and the second is the important one:

1. The budget scales with how long the baseline has actually been sitting, so a
   real outage stops manufacturing a second data gap on top of itself.
2. It is CAPPED, and a digit-drop / digit-insert OCR error is still rejected
   after an arbitrarily long gap. This gate is a data-integrity control: if
   widening it lets a transposition through, bad readings reach Home Assistant
   silently, which is worse than the false rejections it fixes.

Run either way:
    python tests/test_delta_elapsed.py     (no pytest needed; exits non-zero)
    pytest tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import main as m  # noqa: E402

NOMINAL = m.SCRAPE_INTERVAL_SECONDS


class _Broker:
    """Minimal stand-in: _sanity_check only calls get_last()."""

    def __init__(self, prev: dict):
        self._prev = {k: str(v) for k, v in prev.items()}

    def get_last(self, field):
        return self._prev.get(field)


def _age(field: str, seconds: float) -> None:
    """Pretend `field`'s published baseline is `seconds` old."""
    import time as _t
    m._LAST_ACCEPTED_TS[field] = _t.time() - seconds


def _reset():
    m._LAST_ACCEPTED_TS.clear()
    m._DELTA_CONFIRM.clear()
    m._DECREASE_CONFIRM.clear()


# --------------------------------------------------------------------------
# 1. Normal operation is UNCHANGED
# --------------------------------------------------------------------------

def test_no_widening_in_steady_state():
    """Cycles land ~390s against a 300s nominal. Scaling straight off that
    ratio would loosen every gate ~30% permanently. It must not."""
    _reset()
    _age("kesseltemperatur", 390)
    allowed, scale = m._delta_allowance("kesseltemperatur", 25.0)
    assert scale == 1.0, f"steady-state scale must be 1.0, got {scale}"
    assert allowed == 25.0


def test_no_widening_inside_the_grace_window():
    _reset()
    _age("kesseltemperatur", m.DELTA_ELAPSED_GRACE * NOMINAL - 1)
    _, scale = m._delta_allowance("kesseltemperatur", 25.0)
    assert scale == 1.0


def test_cold_start_behaves_exactly_as_before():
    """No recorded baseline time (fresh process) => unscaled."""
    _reset()
    allowed, scale = m._delta_allowance("kesseltemperatur", 25.0)
    assert (allowed, scale) == (25.0, 1.0)


# --------------------------------------------------------------------------
# 2. The real 2026-08-31 wave is no longer rejected
# --------------------------------------------------------------------------

# field, prev, value  — captured verbatim from the sanity_partial log line.
REAL_WAVE = [
    ("kesseltemperatur", 38.0, 69.0),
    ("outside_temperature", 26.0, 16.0),
    ("saugzuggeblaese_h", 28389.4, 28391.9),
    ("lambdasonde_h", 27832.7, 27835.2),
    ("pelletsbetrieb_h", 19652.1, 19654.2),
    ("betriebsstunden_seit_wartung_h", 1804.7, 1806.8),
    ("puffer_temp_top", 40.0, 71.0),
]
GAP_6H4 = 6.4 * 3600


def test_the_real_post_stall_readings_are_now_accepted():
    _reset()
    broker = _Broker({f: prev for f, prev, _ in REAL_WAVE})
    for f, _, _ in REAL_WAVE:
        _age(f, GAP_6H4)
    rejected = m._sanity_check({f: val for f, _, val in REAL_WAVE}, broker)
    assert not rejected, f"still rejecting correct post-gap readings: {rejected}"


def test_the_same_readings_ARE_rejected_without_a_gap():
    """Control: identical values, fresh baseline. Proves the test above is
    exercising the elapsed scaling and not just a loosened constant."""
    _reset()
    broker = _Broker({f: prev for f, prev, _ in REAL_WAVE})
    for f, _, _ in REAL_WAVE:
        _age(f, NOMINAL)
    rejected = m._sanity_check({f: val for f, _, val in REAL_WAVE}, broker)
    assert len(rejected) >= 6, \
        f"expected the wave to still be rejected at a normal interval, got {rejected}"


# --------------------------------------------------------------------------
# 3. MANDATORY: transposition-scale errors still caught after a long gap
# --------------------------------------------------------------------------

# Real OCR failure modes, as (field, prev, misread).
#
# SPLIT DELIBERATELY. An earlier version of this test used only huge misreads
# and passed even with the ceiling removed entirely — SANITY_BOUNDS was
# rejecting them before the delta gate ever ran, so it proved nothing about the
# scaling. The cases that actually exercise the ceiling are IN-BOUNDS misreads
# on wide-range counters, where the delta gate is the only thing standing.

# Rejected by SANITY_BOUNDS regardless of scaling. Kept as a floor, not as
# evidence about the ceiling.
TRANSPOSITIONS_OUT_OF_BOUNDS = [
    ("kesseltemperatur", 62.0, 869.0),      # prefix smear, from the MAX_DELTA comment
    ("outside_temperature", 16.0, 161.0),   # digit insert
    ("puffer_temp_top", 71.0, 711.0),       # digit insert
]

# IN BOUNDS — single high-order digit substitutions on counters whose ranges are
# wide (og_h 0..1e6). Nothing but the delta gate can catch these.
TRANSPOSITIONS_IN_BOUNDS = [
    ("og_h", 36165.0, 46165.0),                     # 3->4 in the ten-thousands place
    ("pelletsbetrieb_h", 19652.1, 19752.1),         # 6->7 in the hundreds place
    ("anzahl_kesselstarts", 9091.0, 9891.0),        # 0->8 in the hundreds place
    ("pelletsverbrauch_kg", 16304.0, 16904.0),      # 3->9 in the hundreds place
    ("og_h", 36165.0, 398831.0),                    # dropped thousands separator
]


def test_transpositions_still_rejected_after_a_very_long_gap():
    """A 24h gap must not become a licence to accept a digit substitution."""
    _reset()
    cases = TRANSPOSITIONS_OUT_OF_BOUNDS + TRANSPOSITIONS_IN_BOUNDS
    for f, prev, bad in cases:
        _reset()
        broker = _Broker({f: prev})
        _age(f, 24 * 3600)
        rejected = m._sanity_check({f: bad}, broker)
        assert f in rejected, \
            f"{f} {prev} -> {bad} slipped through after a 24h gap"


def test_in_bounds_transpositions_are_caught_by_the_DELTA_gate_specifically():
    """The load-bearing one. These values are inside SANITY_BOUNDS, so if the
    ceiling were removed the delta gate would admit them and bad readings would
    reach Home Assistant silently. Asserting the reject REASON is what makes
    this test fail if the ceiling is ever loosened."""
    _reset()
    for f, prev, bad in TRANSPOSITIONS_IN_BOUNDS:
        lo, hi = m.SANITY_BOUNDS[f]
        assert lo <= bad <= hi, f"precondition: {f}={bad} must be inside bounds"
        _reset()
        broker = _Broker({f: prev})
        _age(f, 24 * 3600)
        rejected = m._sanity_check({f: bad}, broker)
        assert f in rejected, f"{f} {prev} -> {bad} not rejected after 24h"
        assert "exceeds" in rejected[f], \
            f"{f} must be rejected BY THE DELTA GATE, got: {rejected[f]}"


def test_scale_is_capped():
    _reset()
    _age("kesseltemperatur", 30 * 24 * 3600)   # a month
    allowed, scale = m._delta_allowance("kesseltemperatur", 25.0)
    assert scale == m.DELTA_ELAPSED_MAX_SCALE, f"uncapped scale {scale}"
    assert allowed == 25.0 * m.DELTA_ELAPSED_MAX_SCALE


def test_counter_gap_within_the_ceiling_is_accepted():
    """Run-hour counters advance at most 1h per elapsed hour. An 8h gap with
    continuous firing is the demanding legitimate case the ceiling is sized on."""
    _reset()
    broker = _Broker({"pelletsbetrieb_h": 19652.1})
    _age("pelletsbetrieb_h", 8 * 3600)
    rejected = m._sanity_check({"pelletsbetrieb_h": 19652.1 + 7.9}, broker)
    assert not rejected, f"legitimate 7.9h advance over an 8h gap rejected: {rejected}"


def test_bounds_still_run_before_any_delta_scaling():
    """Scaling must never rescue an out-of-bounds value."""
    _reset()
    broker = _Broker({"puffer_temp_top": 70.0})
    _age("puffer_temp_top", 24 * 3600)
    rejected = m._sanity_check({"puffer_temp_top": 5.0}, broker)
    assert "puffer_temp_top" in rejected and "out of bounds" in rejected["puffer_temp_top"], \
        f"bounds must still fire first: {rejected}"


def test_ceiling_stays_within_its_documented_derivation():
    """Guard the constant directly.

    The behavioural tests above only bite once the ceiling is loosened past
    ~40x — below that, the smallest realistic in-bounds transposition (a
    hundreds-digit substitution, delta 100 against a 2.0h counter budget) is
    still rejected. That leaves a wide window where a well-meaning bump would
    go unnoticed, so pin the derivation itself:

      run-hour counters advance at most 1h per elapsed hour, against a 2.0h
      per-cycle budget, so covering a gap of G hours needs a scale of G/2.
      4.0 covers ~8h. Every legitimate reading in the observed 2026-08-31
      wave needed at most 1.25x.

    Raising this is a data-integrity decision, not a tuning tweak: it directly
    widens what reaches Home Assistant unchecked. Change the reasoning here in
    the same commit.
    """
    assert 2.0 <= m.DELTA_ELAPSED_MAX_SCALE <= 8.0, (
        f"ceiling {m.DELTA_ELAPSED_MAX_SCALE} is outside its documented "
        "derivation (2.0-8.0); see the constant's comment in main.py")
    assert m.DELTA_ELAPSED_GRACE >= 1.0, \
        "grace below 1.0 would widen the gate during normal operation"


def _run() -> int:
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as e:
                failures += 1
                print(f"FAIL {name}: {e}")
    print(f"\n{'ALL PASS' if not failures else f'{failures} FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(_run())
