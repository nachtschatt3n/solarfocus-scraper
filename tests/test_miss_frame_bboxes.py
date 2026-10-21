#!/usr/bin/env python3
"""Regression fixtures captured by the /screenshot/miss endpoint (63cabee).

Each frame is a real production read that failed, while its value is plainly
legible. These are exactly the frames that could not be obtained before the
endpoint existed, because every capture was overwritten one cycle later.

Run: python tests/test_miss_frame_bboxes.py  or  pytest tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from PIL import Image  # noqa: E402

import main as m  # noqa: E402

FX = ROOT / "tests" / "fixtures"


def _read(field, fixture):
    sp = m.BBOXES[field]
    img = Image.open(FX / fixture).convert("RGB")
    return m.parse_value(m.ocr(img, sp.bbox, sp.config, invert=sp.invert, lcd=sp.lcd), sp.kind)


def test_og_vorlauftemperatur_reads_the_frame_it_missed():
    """2026-10-21 07:31: panel showed 33 °C, the old bbox read nothing."""
    assert _read("og_vorlauftemperatur", "miss_og_vorlauftemperatur_33.png") == 33.0


def test_og_vorlauftemperatur_still_reads_absenkbetrieb():
    assert _read("og_vorlauftemperatur", "screen_heizkreise_og_absenkbetrieb.png") == 31.0


def test_ww_ist_temp_reads_the_frame_it_rejected():
    """2026-10-13 16:45: tank showed 54 °C, the old bbox read 4.0."""
    assert _read("ww_ist_temp", "miss_ww_ist_temp_54.png") == 54.0


def test_ww_ist_temp_still_reads_the_existing_fixtures():
    assert _read("ww_ist_temp", "warmwasser_74.png") == 74.0
    assert _read("ww_ist_temp", "warmwasser_75.png") == 75.0


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
