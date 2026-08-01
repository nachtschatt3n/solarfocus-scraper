"""OCR regression test for the DHW actual-temp field (`ww_ist_temp`).

Incident: the LCD readout box has a full-width dark shadow line just above the
digits; the old bbox (y-start 175) swallowed it, Otsu fused it into a black bar
across the digit tops, and tesseract dropped the 2nd digit ("75"→"7", "74"→"7").
The bogus 7 was rejected by the [20,90] bound every cycle, freezing
`warmwasser_ist` and pinning `scraper_status` at "partial". Fix: crop below the
shadow bar (bbox (168,187,72,22)).

Guards that the shipped FieldSpec reads the real value (and passes sanity) on
both the live "75" frame and the reference "74" frame. Requires tesseract(deu).
Run: `python tests/test_ocr_ww_ist_temp.py` or `pytest`.
"""
from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import main as m  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
FIX = ROOT / "tests" / "fixtures"

CASES = [(FIX / "warmwasser_75.png", 75.0), (FIX / "warmwasser_74.png", 74.0)]


def _read(path: Path) -> float | None:
    spec = m.BBOXES["ww_ist_temp"]
    raw = m.ocr(Image.open(path).convert("RGB"), spec.bbox, spec.config,
                invert=spec.invert, lcd=spec.lcd)
    return m.parse_value(raw, spec.kind)


def test_ww_ist_temp_reads_real_value():
    for path, expected in CASES:
        assert _read(path) == expected, f"{path.name}: expected {expected}"


def test_ww_ist_temp_passes_sanity_bound():
    """The corrected read is inside the [20,90] bound (so status returns to ok,
    warmwasser_ist tracks live) rather than the bogus 7 the bug produced."""
    lo, hi = m.SANITY_BOUNDS["ww_ist_temp"]
    for path, _ in CASES:
        v = _read(path)
        assert v is not None and lo <= v <= hi, f"{path.name}: {v} rejected by [{lo},{hi}]"


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
