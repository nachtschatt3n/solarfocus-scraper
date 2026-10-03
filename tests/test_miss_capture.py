#!/usr/bin/env python3
"""Keep the frame behind an OCR miss, so an intermittent one can be diagnosed.

og_vorlauftemperatur goes missing ~0.7% of cycles while its row is visibly on
screen. Every capture is overwritten one cycle later, so no failing frame was
ever available to fix its bbox against. The coordinator now keeps, per field,
the capture that field last failed to read from.

Run: python tests/test_miss_capture.py  or  pytest tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from PIL import Image  # noqa: E402

import main as m  # noqa: E402


def _capture(screen, colour):
    m.COORD.update_after_capture(screen, Image.new("RGB", (640, 480), colour))


def test_a_miss_keeps_the_frame_it_happened_on():
    m.COORD.miss_captures.clear()
    _capture("heizkreise_og", (10, 20, 30))
    failing = m.COORD.get_screen_png("heizkreise_og")[0]
    m.COORD.record_miss("og_vorlauftemperatur", "heizkreise_og")
    _capture("heizkreise_og", (200, 200, 200))          # next cycle overwrites
    png, _ts, screen = m.COORD.get_miss_png("og_vorlauftemperatur")
    assert png == failing, "the failing frame must survive the next capture"
    assert screen == "heizkreise_og"


def test_no_miss_no_entry():
    m.COORD.miss_captures.clear()
    assert m.COORD.get_miss_png("kesseltemperatur") is None


def test_ocr_all_records_a_miss_for_a_field_that_reads_nothing():
    """A blank OG screen reads nothing in any OG field — each is recorded."""
    m.COORD.miss_captures.clear()
    img = Image.new("RGB", (640, 480), (200, 200, 200))
    m.COORD.update_after_capture("heizkreise_og", img)
    out = m._ocr_all({"heizkreise_og": img})
    assert out["og_vorlauftemperatur"] is None
    assert m.COORD.get_miss_png("og_vorlauftemperatur") is not None


def test_a_successful_read_records_nothing():
    m.COORD.miss_captures.clear()
    img = Image.open(ROOT / "docs" / "screenshots" / "screen-heizkreise-og.png").convert("RGB")
    m.COORD.update_after_capture("heizkreise_og", img)
    out = m._ocr_all({"heizkreise_og": img})
    assert out["og_vorlauftemperatur"] is not None
    assert m.COORD.get_miss_png("og_vorlauftemperatur") is None


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
