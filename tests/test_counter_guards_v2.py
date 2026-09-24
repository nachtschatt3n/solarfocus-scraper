#!/usr/bin/env python3
"""Second round of counter guards, from the 2026-09-24 production review.

The 09-06 fix (repair + >=5x ratio guard) held against every 10x episode in
18 days — 45 rejections, none through. But the review found two paths it did
not close:

1. PRECISION. 512 of 699 repairs divided by 100 and published values the
   display cannot show: saugaustragung_h 567.51 / 571.01 on a one-decimal
   counter, rla_pumpe_h 31883.2 on a whole-hour one. Those were stray glyphs,
   not dropped decimal points; dividing turned a misread into a plausible
   wrong value, and being slightly high it then made correct reads look like
   decreases.

2. THE DECREASE BREAKER. 2026-09-15: rla_pumpe_h 31883 -> 31834, an 83->34
   digit swap, read identically three times and accepted — then served to HA
   for 17.5 h, putting a -49 h / +52 h scar in its long-term statistics. The
   panel often does not redraw between cycles, so three identical OCR reads of
   the same pixels are not independent confirmation.

Run either way:
    python tests/test_counter_guards_v2.py
    pytest tests/
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import main as m  # noqa: E402


class _Broker:
    def __init__(self, prev):
        self._prev = {k: str(v) for k, v in prev.items()}

    def get_last(self, field):
        return self._prev.get(field)


def _reset():
    m._LAST_ACCEPTED_TS.clear()
    m._DELTA_CONFIRM.clear()
    m._DECREASE_CONFIRM.clear()


def _run(field, prev, val, cycles=m.DELTA_CONFIRM_THRESHOLD + 2):
    """Feed the same reading for several cycles, as a stuck OCR would."""
    broker = _Broker({field: prev})
    last = None
    for _ in range(cycles):
        values = {field: val}
        last = m._sanity_check(values, broker, allow_delta_override=True)
        if field not in last:
            # Accepted. A real broker would now hold the new baseline; this
            # fake one does not, so stop rather than start re-counting.
            break
    return last, values


# --------------------------------------------------------------------------
# 1. Precision
# --------------------------------------------------------------------------

def test_no_repair_finer_than_the_display():
    """The first repair published 557.51 / 571.11 — two decimals on a
    one-decimal display. Whatever it returns now, it is never that."""
    _reset()
    for raw, prev in ((55751.0, 557.4), (57111.0, 571.0), (56751.0, 567.4)):
        got = m._counter_scale_repair("saugaustragung_h", raw, prev)
        assert got is None or round(got, 1) == got, got


def test_the_live_0924_misread_recovers_the_true_value():
    """Panel showed "571.1 h"; OCR read 57111 on 5 of 5 reads. Rejecting it
    would freeze the counter for as long as the panel shows that value."""
    _reset()
    assert m._counter_scale_repair("saugaustragung_h", 57111.0, 571.0) == 571.1


def test_every_historical_trailing_glyph_read_resolves_to_the_panel_value():
    """The raws the /100 path got 0.01 high, now resolved correctly."""
    _reset()
    assert m._counter_scale_repair("saugaustragung_h", 55751.0, 557.4) == 557.5
    assert m._counter_scale_repair("saugaustragung_h", 56751.0, 567.4) == 567.5
    assert m._counter_scale_repair("saugaustragung_h", 57101.0, 571.0) == 571.0


def test_the_next_panel_value_unfreezes_a_bad_baseline():
    """The old image left 571.11 as the baseline. 571.1 is below it, so 57111
    cannot repair (correctly — never invent a decrease); the next panel value
    571.2 -> 57121 must repair to 571.2 and move the counter on."""
    _reset()
    assert m._counter_scale_repair("saugaustragung_h", 57111.0, 571.11) is None
    assert m._counter_scale_repair("saugaustragung_h", 57121.0, 571.11) == 571.2


def test_a_sweep_through_the_observed_failure_shape():
    """Panel 571.1 -> 572.0, each read with the point lost and a trailing "1".
    Every one must resolve to exactly the panel value."""
    _reset()
    prev = 571.0
    for tenths in range(5711, 5721):
        true = tenths / 10
        raw = float(f"{true:.1f}".replace(".", "") + "1")
        got = m._counter_scale_repair("saugaustragung_h", raw, prev)
        assert got == true, f"{true:.1f} read as {raw:.0f} -> {got}"
        prev = got


def test_a_different_misread_shape_is_never_published_wrong():
    """If the point were instead read AS a "1" in place (572.3 -> 57213), the
    trailing-glyph repair must not produce a wrong value: it may repair to the
    truth by coincidence or refuse, never anything else."""
    _reset()
    prev = 572.0
    for tenths in range(5721, 5730):
        true = tenths / 10
        raw = float(f"{true:.1f}".replace(".", "1"))
        got = m._counter_scale_repair("saugaustragung_h", raw, prev)
        assert got is None or got == true, f"{true:.1f} read as {raw:.0f} -> {got}"


def test_a_real_dropped_decimal_ending_in_one_is_not_mistaken():
    """6594.1 read as 65941: the /10 shape is the right one, and the trailing
    strip (659.4) is far out of budget, so there is no ambiguity."""
    _reset()
    assert m._counter_scale_repair("einschub_h", 65941.0, 6594.0) == 6594.1


# --------------------------------------------------------------------------
# 2. Bounded decrease breaker
# --------------------------------------------------------------------------

def test_the_0915_digit_swap_can_never_be_confirmed():
    """31883 -> 31834, read identically every cycle."""
    _reset()
    for _ in range(m.DELTA_CONFIRM_THRESHOLD + 5):
        rejected, _ = _run("rla_pumpe_h", 31883.0, 31834.0, cycles=1)
        assert "rla_pumpe_h" in rejected
        assert "not confirmable" in rejected["rla_pumpe_h"]
    assert "rla_pumpe_h" not in m._DECREASE_CONFIRM


def test_a_small_decrease_can_still_unwind_an_inflation():
    """The breaker's legitimate job: a baseline a little too high (e.g. from
    the old /100 repairs, 570.41 against a true 570.3) must still recover."""
    _reset()
    rejected, _ = _run("saugaustragung_h", 570.41, 570.3)
    assert "saugaustragung_h" not in rejected, rejected


def test_decrease_bound_scales_with_the_field_budget():
    """Fuel is budgeted in kg, not hours; the bound follows the field."""
    _reset()
    rejected, _ = _run("pelletsverbrauch_kg", 16400.0, 16380.0)   # -20 kg
    assert "pelletsverbrauch_kg" not in rejected
    _reset()
    rejected, _ = _run("pelletsverbrauch_kg", 16400.0, 16300.0)   # -100 kg
    assert "not confirmable" in rejected["pelletsverbrauch_kg"]


# --------------------------------------------------------------------------
# 3. An hour meter cannot outrun the clock
# --------------------------------------------------------------------------

def test_a_stable_upward_misread_cannot_be_confirmed():
    """Without this, the bounded decrease breaker could leave a counter frozen
    on an inflated value forever. +49 h in 10 minutes is physically impossible."""
    _reset()
    m._LAST_ACCEPTED_TS["rla_pumpe_h"] = time.time() - 600
    rejected, _ = _run("rla_pumpe_h", 31834.0, 31883.0)
    assert "rla_pumpe_h" in rejected
    assert "outrun the clock" in rejected["rla_pumpe_h"]
    assert "rla_pumpe_h" not in m._DELTA_CONFIRM


def test_a_genuine_catch_up_after_a_long_outage_still_passes():
    """The per-cycle budget is capped (DELTA_ELAPSED_MAX_SCALE); the physical
    bound is not. 11 h of run time after a 12 h outage is real and must be
    confirmable — this is why the breaker exists at all."""
    _reset()
    m._LAST_ACCEPTED_TS["pelletsbetrieb_h"] = time.time() - 12 * 3600
    rejected, _ = _run("pelletsbetrieb_h", 19662.0, 19673.0)
    assert "pelletsbetrieb_h" not in rejected, rejected


def test_unknown_accept_time_keeps_legacy_behaviour():
    """After a restart _LAST_ACCEPTED_TS is empty; the physical cap cannot be
    computed and must not guess. The per-cycle budget still applies."""
    _reset()
    rejected, _ = _run("pelletsbetrieb_h", 19662.0, 19665.0)
    assert "outrun the clock" not in str(rejected)


def test_non_hour_counters_are_not_clock_capped():
    """Kesselstarts and kg are not hours; wall-clock time says nothing."""
    _reset()
    m._LAST_ACCEPTED_TS["anzahl_kesselstarts"] = time.time() - 60
    rejected, _ = _run("anzahl_kesselstarts", 9095.0, 9120.0)
    assert "outrun the clock" not in str(rejected)


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

