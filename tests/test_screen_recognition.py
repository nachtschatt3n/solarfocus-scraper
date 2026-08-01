"""Screen-recognition regression tests.

Guards the info-modal fix (incident 2026-07-31): the heater's WARTUNG-INSPEKTION
maintenance-reminder dialog must be recognised as `alert_modal` (so it is
dismissed instead of aborting navigation), while every ordinary data screen must
keep resolving to its own name and must NOT be misclassified as an alert modal.

Requires tesseract (deu) — same as running the scraper. No pytest dependency
needed to run: `python tests/test_screen_recognition.py` executes the checks and
exits non-zero on failure. `pytest tests/` also works.
"""
from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import main as m  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
MODAL = ROOT / "tests" / "fixtures" / "alert_modal_wartung.png"

# Known data screens (committed, also used in the README) and their expected
# identification. None = "don't care which, but it must NOT be alert_modal".
DATA_SCREENS = {
    "screenshots/cal-probe.png": "automatische_saugsondenumschalteinheit",
    "screenshots/cal-saugaustragung.png": "saugaustragung",
    "docs/screenshots/screen-kessel.png": "kessel",
    "docs/screenshots/screen-heizkreise-og.png": "heizkreise_og",
    "docs/screenshots/screen-fussbodenheizung.png": "heizkreise_fbh",
    "docs/screenshots/screen-warmwasser.png": "warmwasser",
    "docs/screenshots/screen-betriebsstunden-p3.png": "betriebsstunden_p3",
    "docs/screenshots/current-heater-screen.png": None,
}


def _load(rel: str) -> Image.Image:
    return Image.open(ROOT / rel).convert("RGB")


def test_wartung_modal_recognised_as_alert_modal():
    """The WARTUNG-INSPEKTION reminder is recognised (via the icon fallback)."""
    img = Image.open(MODAL).convert("RGB")
    assert m.looks_like_info_modal(img) is True
    assert m._identify_screen(img) == "alert_modal"


def test_ok_button_dismiss_coordinate_inside_button():
    """The alert_modal dismiss tap must land on the OK button (x245-384/y400-464)."""
    x, y = m.SCREENS["alert_modal"].back_xy
    assert 245 <= x <= 384 and 400 <= y <= 464


def test_data_screens_not_misidentified_as_modal():
    """No ordinary data screen is a false positive for the info-modal detector,
    and each still resolves to its own screen name."""
    for rel, expected in DATA_SCREENS.items():
        img = _load(rel)
        assert m.looks_like_info_modal(img) is False, f"{rel} false-positive modal"
        got = m._identify_screen(img)
        assert got != "alert_modal", f"{rel} misidentified as alert_modal"
        if expected is not None:
            assert got == expected, f"{rel}: expected {expected}, got {got}"


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
