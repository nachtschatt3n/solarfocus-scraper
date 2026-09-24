#!/usr/bin/env python3
"""Residual O2 read without its decimal point during burns.

`restsauerstoffgehalt` displays one decimal ("7.4 %") and is physically at most
21 %, bounded (0, 25). During burns OCR regularly returns 74, 78, 90, 91, 98 —
the point dropped. Bounds rejected every one and the value was thrown away, so
Home Assistant kept showing the standby 21 % through the burn.

A whole-number read above the bound can only be the point dropped; its tenth is
restored. Nothing within the bound is ever touched.

Run: python tests/test_o2_dropped_decimal.py  or  pytest tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import main as m  # noqa: E402

F = "restsauerstoffgehalt"


def test_observed_burn_reads_are_restored():
    for raw, want in ((74.0, 7.4), (78.0, 7.8), (90.0, 9.0), (91.0, 9.1), (98.0, 9.8)):
        assert m._repair_dropped_decimal(F, raw) == want, raw


def test_in_range_values_are_never_reinterpreted():
    """Standby reads 21.0; a dropped-point 2.1 would also read 21. Inside the
    bound the two are indistinguishable, so it is left exactly as read."""
    for raw in (21.0, 20.0, 6.1, 7.4, 0.0, 25.0):
        assert m._repair_dropped_decimal(F, raw) == raw


def test_nonsense_beyond_ten_times_the_bound_is_left_for_bounds_to_reject():
    assert m._repair_dropped_decimal(F, 870.0) == 870.0


def test_values_that_already_carry_a_decimal_are_not_touched():
    assert m._repair_dropped_decimal(F, 74.5) == 74.5


def test_other_fields_are_untouched():
    """ww_ist_temp's "8.0" is a dropped DIGIT (80 -> 8), not a dropped point;
    this repair must not apply to it or to any field outside the allow-list."""
    assert m._repair_dropped_decimal("ww_ist_temp", 800.0) == 800.0
    assert m._repair_dropped_decimal("kesseltemperatur", 620.0) == 620.0


def test_repaired_value_still_goes_through_sanity():
    """The repair recovers a reading; it does not bypass the pipeline. A jump
    from standby 21 to 7.4 still has to clear the delta gate."""
    class B:
        def get_last(self, f):
            return "21.0"
    m._DELTA_CONFIRM.clear()
    m._LAST_ACCEPTED_TS.clear()
    rejected = m._sanity_check({F: m._repair_dropped_decimal(F, 74.0)}, B())
    assert F in rejected and "exceeds" in rejected[F]


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn(); print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1; print(f"FAIL {name}: {exc}")
    print("ALL PASS" if not failures else f"{failures} FAILED")
    raise SystemExit(1 if failures else 0)
