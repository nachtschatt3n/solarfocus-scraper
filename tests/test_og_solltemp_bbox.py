#!/usr/bin/env python3
"""og_vorlaufsolltemperatur must read in Heizbetrieb and stay None in Absenkbetrieb.

Background (2026-09-01). The field's bbox sat at y=355 h=22, roughly 5px too
low, so it clipped the digits and returned an empty string on the OG heating
circuit's screen. `_sanity_check` never saw it — a None lands in neither
`accepted` nor `rejected` — so it hid inside a normal-looking accepted_count,
while Home Assistant kept serving a stale retained 29.0 as though it were live.

The subtlety this file exists to pin: the field is None for TWO different
reasons, and only one is a bug.

  * HEIZBETRIEB  — the heater renders the setpoint row. Reading it is required.
  * ABSENKBETRIEB — the heater does NOT render the row at all (label and value
    both absent). None is the CORRECT answer, and a widened/moved bbox must not
    start scraping whatever pixels happen to sit at those coordinates.

Fixing only the first and testing only the first would look like success while
silently converting "correctly absent" into "confidently wrong".

Run either way:
    python tests/test_og_solltemp_bbox.py     (no pytest needed; exits non-zero)
    pytest tests/
Needs tesseract with the `deu` language pack, same as the scraper.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from PIL import Image  # noqa: E402

import main as m  # noqa: E402

HEIZBETRIEB = ROOT / "docs" / "screenshots" / "screen-heizkreise-og.png"
ABSENKBETRIEB = ROOT / "tests" / "fixtures" / "screen_heizkreise_og_absenkbetrieb.png"

FIELD = "og_vorlaufsolltemperatur"


def _ocr_field(path: Path, field: str):
    """Drive the real _ocr_all path, so parse_value() is exercised too."""
    spec = m.BBOXES[field]
    return m._ocr_all({spec.screen: Image.open(path)})[field]


def _status(path: Path) -> str:
    spec = m.BBOXES["og_heizkreis_status"]
    return m.ocr(Image.open(path), spec.bbox, spec.config, invert=spec.invert).strip()


# --------------------------------------------------------------------------
# Preconditions — the fixtures really are the two modes.
# --------------------------------------------------------------------------

def test_fixtures_are_the_two_modes():
    assert "Heizbetrieb" in _status(HEIZBETRIEB), \
        f"Heizbetrieb fixture no longer shows heating: {_status(HEIZBETRIEB)!r}"
    assert "Absenkbetrieb" in _status(ABSENKBETRIEB), \
        f"Absenkbetrieb fixture no longer shows setback: {_status(ABSENKBETRIEB)!r}"


# --------------------------------------------------------------------------
# The bug, and the thing that must not break while fixing it.
# --------------------------------------------------------------------------

def test_heizbetrieb_reads_the_setpoint():
    """This is what was broken: value plainly legible on screen, read as empty."""
    val = _ocr_field(HEIZBETRIEB, FIELD)
    assert val is not None, "setpoint row is rendered in Heizbetrieb and must be read"
    assert isinstance(val, float), f"expected a float, got {val!r}"
    assert 20.0 <= val <= 90.0, f"implausible flow setpoint {val}"


def test_absenkbetrieb_stays_none():
    """The row is absent from the panel in setback. Reading anything here would
    mean the bbox is scraping unrelated pixels."""
    assert _ocr_field(ABSENKBETRIEB, FIELD) is None, \
        "setpoint row is NOT rendered in Absenkbetrieb — any value is a misread"


def test_neighbouring_rows_unaffected_in_both_modes():
    """The bbox moved up 7px and grew 4px; it must not have eaten a neighbour."""
    for path in (HEIZBETRIEB, ABSENKBETRIEB):
        for neighbour in ("og_vorlauftemperatur", "og_mischerposition"):
            val = _ocr_field(path, neighbour)
            assert val is not None, f"{neighbour} broke on {path.name}"
            assert isinstance(val, float)


def test_bbox_does_not_overlap_its_neighbours():
    """Structural guard, independent of OCR: keeps a future nudge honest."""
    def span(f):
        _, y, _, h = m.BBOXES[f].bbox
        return y, y + h
    above = span("og_vorlauftemperatur")
    self_ = span(FIELD)
    below = span("og_mischerposition")
    assert above[1] < self_[0], f"overlaps the row above: {above} vs {self_}"
    assert self_[1] < below[0], f"overlaps the row below: {self_} vs {below}"


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
