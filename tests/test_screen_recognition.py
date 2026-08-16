"""Screen-recognition regression tests.

Guards two info-modal incidents:

* 2026-07-31 — the WARTUNG-INSPEKTION reminder (full-screen dialog) was not
  recognised, so navigation aborted on an "unknown" screen.
* 2026-08-16 — the PELLETSMANGEL IM LAGERRAUM reminder (inset dialog, drawn
  over Saugaustragung ~50px lower) was recognised as the screen UNDERNEATH it,
  so navigation clicked inert pixels for 3.5h with no self-recovery.

Both variants must resolve to `alert_modal`, every ordinary data screen must
keep resolving to its own name and must NOT be a false positive for the modal
detector, the dismiss tap must be derived from the detected geometry (and must
never be an action button), and navigate_to must bail out of an inert-click
loop by itself.

Requires tesseract (deu) — same as running the scraper. No pytest dependency
needed to run: `python tests/test_screen_recognition.py` executes the checks and
exits non-zero on failure. `pytest tests/` also works.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import main as m  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
MODAL_FULLSCREEN = ROOT / "tests" / "fixtures" / "alert_modal_wartung.png"
MODAL_INSET = ROOT / "tests" / "fixtures" / "alert_modal_pelletsmangel_inset.png"
MODAL_FIXTURES = {MODAL_FULLSCREEN, MODAL_INSET}

# Known data screens (committed, also used in the README) and their expected
# identification. None = "don't care which, but it must NOT be alert_modal".
DATA_SCREENS = {
    "tests/fixtures/screen_probe.png": "automatische_saugsondenumschalteinheit",
    # The host screen from the 2026-08-16 incident — the inset modal is drawn
    # over exactly this, and its title bar stays visible underneath.
    "tests/fixtures/screen_saugaustragung.png": "saugaustragung",
    "docs/screenshots/screen-kessel.png": "kessel",
    "docs/screenshots/screen-heizkreise-og.png": "heizkreise_og",
    "docs/screenshots/screen-fussbodenheizung.png": "heizkreise_fbh",
    "docs/screenshots/screen-warmwasser.png": "warmwasser",
    "docs/screenshots/screen-betriebsstunden-p3.png": "betriebsstunden_p3",
    "docs/screenshots/current-heater-screen.png": None,
}

# Measured button boxes on the inset PELLETSMANGEL dialog (x0, y0, x1, y1).
# The two bottom buttons are ACTION buttons — the right one is
# "Lagerraum befüllt", which tells the heater the pellet store was refilled.
# Escaping the dialog must never land in either.
INSET_BACK_ARROW_BOX = (17, 83, 90, 145)
INSET_ACTION_BUTTON_BOXES = ((19, 359, 186, 458), (451, 359, 618, 415))
# The full-screen WARTUNG dialog's only exit: a single centred OK button.
FULLSCREEN_OK_BOX = (245, 400, 384, 464)


def _load(rel: str) -> Image.Image:
    return Image.open(ROOT / rel).convert("RGB")


def _in_box(xy: tuple[int, int], box: tuple[int, int, int, int]) -> bool:
    x, y = xy
    x0, y0, x1, y1 = box
    return x0 <= x <= x1 and y0 <= y <= y1


# Committed fixture directories. `screenshots/` is deliberately NOT in this
# list: it is the gitignored scratch dir written by `main.py probe/click/
# explore`, so it is empty in a fresh clone. It is still scanned opportunis-
# tically below for extra local coverage, but nothing asserts on it.
CORPUS_DIRS = ("docs/screenshots", "tests/fixtures")
SCRATCH_DIR = "screenshots"


def _corpus(include_scratch: bool = True) -> list[Path]:
    """Every 640x480 heater screenshot available, modals included."""
    dirs = list(CORPUS_DIRS) + ([SCRATCH_DIR] if include_scratch else [])
    out: list[Path] = []
    for d in dirs:
        for p in sorted((ROOT / d).glob("*.png")):
            if Image.open(p).size == (640, 480):
                out.append(p)
    return out


# --------------------------------------------------------------------------
# Detection
# --------------------------------------------------------------------------

def test_fullscreen_modal_recognised_as_alert_modal():
    """The WARTUNG-INSPEKTION reminder is recognised (via the icon detector)."""
    img = Image.open(MODAL_FULLSCREEN).convert("RGB")
    assert m.looks_like_info_modal(img) is True
    assert m._identify_screen(img) == "alert_modal"


def test_inset_modal_recognised_as_alert_modal():
    """The PELLETSMANGEL reminder sits ~50px lower than the full-screen variant.

    The detector must locate the icon by scanning rather than at a fixed box —
    the old one counted 0 blue pixels here against a required 150.
    """
    img = Image.open(MODAL_INSET).convert("RGB")
    assert m.looks_like_info_modal(img) is True
    assert m._identify_screen(img) == "alert_modal"


def test_inset_modal_wins_over_the_screen_underneath_it():
    """Regression for the 3.5h stall: the host screen's fingerprint is STILL
    valid through the inset dialog, so an overlay must be checked first.

    This asserts the trap is real (saugaustragung's title-bar hash matches the
    modal capture exactly) and that _identify_screen returns the overlay anyway.
    """
    img = Image.open(MODAL_INSET).convert("RGB")
    host = m.SCREENS["saugaustragung"]
    assert m.region_hash(img, host.hash_region) == host.expected_hash, \
        "fixture no longer reproduces the host-screen hash collision"
    assert m._identify_screen(img) == "alert_modal"


def test_modal_icon_located_at_the_variant_specific_offset():
    """Both variants are found, at their own offsets — the whole point of the
    anchor-relative detector."""
    full = m.find_info_modal(Image.open(MODAL_FULLSCREEN).convert("RGB"))
    inset = m.find_info_modal(Image.open(MODAL_INSET).convert("RGB"))
    assert full is not None and inset is not None
    assert full.icon_box[1] != inset.icon_box[1], "variants must differ in y"
    assert abs(inset.icon_box[1] - full.icon_box[1]) >= 40


# --------------------------------------------------------------------------
# False positives
# --------------------------------------------------------------------------

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


def test_no_false_positive_across_the_whole_fixture_corpus():
    """Broader than DATA_SCREENS: every committed 640x480 capture that is not a
    known modal must not trip the detector. Several of these screens carry big
    blue title bars and blue buttons — the checks that reject them (icon
    fill-fraction, white margins beside the icon, white dialog body) are what
    this test defends."""
    checked = committed = 0
    for p in _corpus():
        if p in MODAL_FIXTURES:
            continue
        assert m.looks_like_info_modal(Image.open(p).convert("RGB")) is False, \
            f"{p.relative_to(ROOT)} false-positive modal"
        checked += 1
        if p.parent != ROOT / SCRATCH_DIR:
            committed += 1
    assert committed >= 10, \
        f"committed corpus shrank unexpectedly ({committed} screens, {checked} scanned)"


# --------------------------------------------------------------------------
# Dismiss geometry
# --------------------------------------------------------------------------

def test_modal_dismiss_prefers_the_back_arrow_over_action_buttons():
    """The inset dialog has a back arrow AND two action buttons. Escaping must
    use the back arrow: it is pure navigation, whereas the bottom-right button
    is "Lagerraum befüllt" and clicking it would falsify the heater's
    pellet-store state."""
    img = Image.open(MODAL_INSET).convert("RGB")
    modal = m.find_info_modal(img)
    assert modal is not None
    assert modal.dismiss_via == "back_arrow"
    assert modal.dismiss_xy is not None
    assert _in_box(modal.dismiss_xy, INSET_BACK_ARROW_BOX), \
        f"dismiss {modal.dismiss_xy} outside the back-arrow button"
    for box in INSET_ACTION_BUTTON_BOXES:
        assert not _in_box(modal.dismiss_xy, box), \
            f"dismiss {modal.dismiss_xy} landed on action button {box}"
    assert m._dismiss_xy_for(img, "alert_modal") == modal.dismiss_xy


def test_ok_button_dismiss_coordinate_inside_button():
    """The acknowledge-only dialog has no back arrow, so its single centred OK
    button is the exit — and the tap must land inside it.

    Reworked from a hardcoded SCREENS["alert_modal"].back_xy assertion: the
    coordinate is now DERIVED from the detected dialog, so the same assertion
    is made against the derived value. A fixed coordinate could not work — the
    old (320, 410) is empty gap between the inset variant's two buttons.
    """
    img = Image.open(MODAL_FULLSCREEN).convert("RGB")
    modal = m.find_info_modal(img)
    assert modal is not None
    assert modal.dismiss_via == "ok_button"
    assert modal.dismiss_xy is not None
    assert _in_box(modal.dismiss_xy, FULLSCREEN_OK_BOX), \
        f"dismiss {modal.dismiss_xy} outside the OK button {FULLSCREEN_OK_BOX}"
    assert m._dismiss_xy_for(img, "alert_modal") == modal.dismiss_xy


def test_alert_modal_screen_has_no_hardcoded_dismiss_coordinate():
    """`back_xy` must stay at the shared default for alert_modal — a per-variant
    dismiss point cannot be expressed as one constant, and pinning one here is
    exactly how the inset dialog got clicked in an empty gap."""
    assert m.SCREENS["alert_modal"].back_xy == m.BACK_ARROW_XY


# --------------------------------------------------------------------------
# Stuck-screen guard
# --------------------------------------------------------------------------

class _EventCatcher(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        et = getattr(record, "event_type", None)
        if et:
            self.events.append(et)


def _navigate_with_frozen_screen(fixture: str, target: str):
    """Run navigate_to against a capture that never changes, recording clicks."""
    img = Image.open(ROOT / fixture).convert("RGB")
    clicks: list[tuple[int, int]] = []
    catcher = _EventCatcher()
    orig = (m.vnc_capture, m.vnc_click, m.time.sleep)
    m.vnc_capture = lambda client, save_path=None: img.copy()
    m.vnc_click = lambda client, x, y: clicks.append((x, y))
    m.time.sleep = lambda s: None
    m.log.addHandler(catcher)
    try:
        ok = m.navigate_to(object(), target)
    finally:
        m.vnc_capture, m.vnc_click, m.time.sleep = orig
        m.log.removeHandler(catcher)
    return ok, clicks, catcher.events


def test_navigate_stuck_screen_guard_fires_on_inert_clicks():
    """The safety net. A screen that stays identified as itself no matter how
    often it is clicked must abort with a DISTINGUISHABLE event, not burn
    max_steps and log the generic navigate_max_steps (which is what made the
    2026-08-16 incident undiagnosable from logs)."""
    ok, clicks, events = _navigate_with_frozen_screen(
        "tests/fixtures/screen_saugaustragung.png", "kessel")
    assert ok is False
    assert "navigate_stuck_screen" in events, f"events seen: {events}"
    assert "navigate_max_steps" not in events, \
        "guard must pre-empt the generic step-exhaustion path"
    assert len(clicks) == m.NAV_STUCK_THRESHOLD - 1, \
        f"guard fired after {len(clicks)} inert clicks"


def test_navigate_stuck_screen_guard_would_have_caught_the_incident():
    """Modal-agnostic: even with the detector removed, the frozen inset-modal
    capture must not click forever. This is the net that catches this class of
    failure regardless of detector quality."""
    orig = m.looks_like_info_modal
    m.looks_like_info_modal = lambda img: False   # simulate a blind detector
    try:
        ok, clicks, events = _navigate_with_frozen_screen(
            "tests/fixtures/alert_modal_pelletsmangel_inset.png", "kessel")
    finally:
        m.looks_like_info_modal = orig
    assert ok is False
    assert "navigate_stuck_screen" in events, f"events seen: {events}"
    assert len(clicks) < 12, "must not exhaust max_steps"


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
