#!/usr/bin/env python3
"""UI-language guard (incident 2026-10-11..13: heater UI left German).

What is pinned here:

1. DETECT   — the classifier calls an English-labelled frame `foreign`, every
   committed real German screen `german` (or at worst not-foreign), and a blank
   frame `indeterminate`.  NEGATIVE CONTROLS prove the detector can fail:
   with the foreign vocabulary emptied the very same English frame is NOT
   flagged, and a German frame is never flagged whatever the foreign list says.
2. FAIL LOUD — N consecutive foreign frames confirm; a single German frame
   resets; the nav-fail hook tags the cycle and publishes retained state.
3. REASSERT — never clicks without an operator-supplied, validated path; never
   clicks in dry-run; never clicks when the anchor check fails; never more than
   2 attempts per episode (also across a simulated restart); hard stop after
   that; budget restored only by an observed German frame.

HONESTY NOTE: no capture of a non-German heater UI exists in this repo, so the
"English" frames below are SYNTHETIC (text rendered with PIL on a heater-style
header). They prove the logic, not the real English strings — extend
FOREIGN_ANCHORS from a real capture next time the UI flips.

Run: `python tests/test_ui_language_guard.py` or `pytest tests/`.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import main as m  # noqa: E402

NAVY = (10, 20, 110)


def _frame(header: str, body: list[str] | None = None) -> Image.Image:
    img = Image.new("RGB", (640, 480), (200, 200, 205))
    d = ImageDraw.Draw(img)
    d.rectangle((80, 5, 635, 40), fill=NAVY)
    font = ImageFont.load_default(size=22)
    d.text((100, 10), header, fill=(255, 255, 255), font=font)
    for i, line in enumerate(body or []):
        d.text((140, 120 + 40 * i), line, fill=(0, 0, 0), font=font)
    return img


def _english() -> Image.Image:
    return _frame("Selection menu", ["Boiler temperature", "Heating circuit", "Hot water tank"])


def _german() -> Image.Image:
    return _frame("Auswahlmenü", ["Kesseltemperatur", "Heizkreis", "Trinkwasserspeicher"])


def _blank() -> Image.Image:
    return Image.new("RGB", (640, 480), (200, 200, 205))


try:                                    # restore module globals between tests
    import pytest

    @pytest.fixture(autouse=True)
    def _restore_globals():
        saved = (m.UI_LANGUAGE_STATE_FILE, m.LANG_TRACKER, m.FOREIGN_ANCHORS,
                 m.UI_LANGUAGE_REASSERT_MIN_INTERVAL_SECONDS)
        yield
        (m.UI_LANGUAGE_STATE_FILE, m.LANG_TRACKER, m.FOREIGN_ANCHORS,
         m.UI_LANGUAGE_REASSERT_MIN_INTERVAL_SECONDS) = saved
except ImportError:                     # plain `python tests/...` run
    pass


# --------------------------------------------------------------- 1. detector

def test_english_frame_is_foreign():
    assert m.classify_ui_language(_english()) == m.LANG_FOREIGN


def test_german_frame_is_german():
    assert m.classify_ui_language(_german()) == m.LANG_GERMAN


def test_blank_frame_is_indeterminate():
    assert m.classify_ui_language(_blank()) == m.LANG_INDETERMINATE


def test_every_real_german_fixture_is_never_foreign():
    paths = [ROOT / "tests/fixtures/screen_probe.png",
             ROOT / "tests/fixtures/screen_saugaustragung.png",
             ROOT / "tests/fixtures/screen_main_alarm_banner.png",
             ROOT / "tests/fixtures/screen_heizkreise_og_absenkbetrieb.png",
             ROOT / "tests/fixtures/warmwasser_74.png",
             ROOT / "docs/screenshots/screen-kessel.png",
             ROOT / "docs/screenshots/screen-heizkreise-og.png",
             ROOT / "docs/screenshots/screen-fussbodenheizung.png",
             ROOT / "docs/screenshots/screen-warmwasser.png",
             ROOT / "docs/screenshots/screen-betriebsstunden-p3.png"]
    for p in paths:
        k = m.classify_ui_language(Image.open(p).convert("RGB"))
        assert k == m.LANG_GERMAN, f"{p.name}: real German screen classified {k}"


def test_negative_control_detector_can_fail():
    """The detector is not 'always foreign': empty the vocabulary and the same
    English frame stops being flagged; and German can never be flagged."""
    orig = m.FOREIGN_ANCHORS
    try:
        m.FOREIGN_ANCHORS = ()
        assert m.classify_ui_language(_english()) != m.LANG_FOREIGN
        m.FOREIGN_ANCHORS = orig + ("auswahlmenü", "kesseltemperatur")
        assert m.classify_ui_language(_german()) == m.LANG_GERMAN   # German anchors win
    finally:
        m.FOREIGN_ANCHORS = orig


# --------------------------------------------------------------- 2. tracker

def _fresh_tracker(tmp: Path):
    m.UI_LANGUAGE_STATE_FILE = tmp / "ep.json"
    m.LANG_TRACKER = m.UiLanguageTracker()
    return m.LANG_TRACKER


def test_tracker_confirms_after_n_and_resets_on_german():
    with tempfile.TemporaryDirectory() as td:
        t = _fresh_tracker(Path(td))
        for _ in range(m.UI_LANGUAGE_FOREIGN_CONFIRMATIONS - 1):
            t.note(m.LANG_FOREIGN)
        assert not t.confirmed_foreign
        t.note(m.LANG_INDETERMINATE)          # neither confirms nor clears
        assert not t.confirmed_foreign
        t.note(m.LANG_FOREIGN)
        assert t.confirmed_foreign
        t.note(m.LANG_GERMAN)
        assert not t.confirmed_foreign and t.foreign == 0


def test_navigate_to_feeds_tracker_and_nav_fail_tags_cycle():
    """Real navigate_to over a never-changing English frame: the walker aborts
    (as in the incident) and the hook then names the cause."""
    with tempfile.TemporaryDirectory() as td:
        _fresh_tracker(Path(td))
        img = _english()
        orig = (m.vnc_capture, m.vnc_click, m.time.sleep, m.UI_LANGUAGE_REASSERT_MODE)
        m.vnc_capture = lambda client, save_path=None: img.copy()
        m.vnc_click = lambda client, x, y: None
        m.time.sleep = lambda s: None
        m.UI_LANGUAGE_REASSERT_MODE = "off"
        try:
            assert m.navigate_to(object(), "kessel") is False
            assert m.LANG_TRACKER.confirmed_foreign
            tag = m.ui_language_after_nav_fail(object(), None, True)
            assert tag == "ui_language_not_german"
        finally:
            m.vnc_capture, m.vnc_click, m.time.sleep, m.UI_LANGUAGE_REASSERT_MODE = orig


def test_german_nav_fail_is_not_blamed_on_language():
    """Control: a jammed German screen must not be reported as a language flip."""
    with tempfile.TemporaryDirectory() as td:
        t = _fresh_tracker(Path(td))
        t.note(m.LANG_GERMAN)
        assert m.ui_language_after_nav_fail(object(), None, True) is None


# --------------------------------------------------------------- 3. reassert

STEPS = json.dumps([
    {"label": "open", "xy": [100, 100], "anchor_region": [85, 5, 555, 36], "anchor_any": ["selection"]},
    {"label": "pick", "xy": [200, 200], "anchor_region": [85, 5, 555, 36], "anchor_any": ["selection"]},
])


class _Rig:
    """Fake heater: records clicks; `recover_after` = attempt number whose path
    completion flips the UI to German (None = never)."""
    def __init__(self, recover_after=None, anchor_img=None):
        self.clicks: list[tuple[int, int]] = []
        self.recover_after = recover_after
        self.anchor_img = anchor_img or _english()
        self.paths_done = 0

    def __enter__(self):
        self.orig = (m.vnc_capture, m.vnc_click, m.time.sleep, m.navigate_to,
                     m.UI_LANGUAGE_REASSERT_MIN_INTERVAL_SECONDS)
        m.vnc_capture = lambda client, save_path=None: self.anchor_img.copy()
        m.vnc_click = lambda client, x, y: self.clicks.append((x, y))
        m.time.sleep = lambda s: None
        m.UI_LANGUAGE_REASSERT_MIN_INTERVAL_SECONDS = 0.0

        def fake_nav(client, target, max_steps=12):
            if target == "main":
                return True
            self.paths_done += 1     # verification navigate_to(auswahlmenue)
            return self.recover_after is not None and self.paths_done >= self.recover_after
        m.navigate_to = fake_nav
        return self

    def __exit__(self, *a):
        (m.vnc_capture, m.vnc_click, m.time.sleep, m.navigate_to,
         m.UI_LANGUAGE_REASSERT_MIN_INTERVAL_SECONDS) = self.orig


def _ep(td):
    m.UI_LANGUAGE_STATE_FILE = Path(td) / "ep.json"
    m.LANG_TRACKER = m.UiLanguageTracker()


def test_no_path_configured_never_clicks():
    with tempfile.TemporaryDirectory() as td, _Rig() as rig:
        _ep(td)
        assert m._reassert_ui_language(object(), steps_raw="", mode="on") == "not_configured"
        assert rig.clicks == []


def test_invalid_path_refused():
    bad = json.dumps([{"xy": [900, 10], "anchor_region": [0, 0, 10, 10], "anchor_any": ["x"]}])
    no_anchor = json.dumps([{"xy": [10, 10], "anchor_region": [0, 0, 10, 10]}])
    with tempfile.TemporaryDirectory() as td, _Rig() as rig:
        _ep(td)
        for raw in (bad, no_anchor, "not json", "[]"):
            assert m._reassert_ui_language(object(), steps_raw=raw, mode="on") == "invalid_config"
        assert rig.clicks == []


def test_dry_run_never_clicks_and_spends_no_budget():
    with tempfile.TemporaryDirectory() as td, _Rig() as rig:
        _ep(td)
        for _ in range(3):
            assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="dry-run") == "dry_run"
        assert rig.clicks == []
        assert m._episode_load()["attempts"] == 0


def test_anchor_mismatch_aborts_before_any_click():
    with tempfile.TemporaryDirectory() as td, _Rig(anchor_img=_german()) as rig:
        _ep(td)
        assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="on") == "aborted_anchor"
        assert rig.clicks == []                       # attempt consumed, zero clicks


def test_two_attempts_then_hard_stop():
    with tempfile.TemporaryDirectory() as td, _Rig(recover_after=None) as rig:
        _ep(td)
        assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="on") == "not_recovered"
        assert len(rig.clicks) == 2
        assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="on") == "halted"
        assert len(rig.clicks) == 4                   # exactly 2 attempts x 2 steps
        for _ in range(5):                            # hard stop: no further clicks, ever
            assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="on") == "halted"
        assert len(rig.clicks) == 4


def test_budget_survives_restart():
    with tempfile.TemporaryDirectory() as td, _Rig(recover_after=None) as rig:
        _ep(td)
        m._reassert_ui_language(object(), steps_raw=STEPS, mode="on")
        m._reassert_ui_language(object(), steps_raw=STEPS, mode="on")
        before = len(rig.clicks)
        m.LANG_TRACKER = m.UiLanguageTracker()        # "restart": memory gone, file stays
        assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="on") == "halted"
        assert len(rig.clicks) == before


def test_recovery_after_first_attempt_and_budget_restored():
    with tempfile.TemporaryDirectory() as td, _Rig(recover_after=1) as rig:
        _ep(td)
        assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="on") == "recovered"
        assert len(rig.clicks) == 2
        assert m._episode_load() == {"attempts": 0, "last_ts": 0.0, "halted": False}


def test_halt_cleared_only_by_german_evidence():
    with tempfile.TemporaryDirectory() as td, _Rig(recover_after=None) as rig:
        _ep(td)
        m._reassert_ui_language(object(), steps_raw=STEPS, mode="on")
        m._reassert_ui_language(object(), steps_raw=STEPS, mode="on")
        assert m._episode_load()["halted"]
        m.LANG_TRACKER.note(m.LANG_FOREIGN)
        assert m._episode_load()["halted"]            # foreign frames don't clear it
        m.LANG_TRACKER.note(m.LANG_GERMAN)            # operator fixed it by hand
        assert not m._episode_load()["halted"]
        assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="on") == "not_recovered"
        assert len(rig.clicks) == 6


def test_min_interval_throttles_second_attempt():
    with tempfile.TemporaryDirectory() as td, _Rig(recover_after=None) as rig:
        _ep(td)
        m.UI_LANGUAGE_REASSERT_MIN_INTERVAL_SECONDS = 600.0
        assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="on") == "not_recovered"
        assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="on") == "throttled"
        assert len(rig.clicks) == 2


def test_unwritable_state_means_no_clicks():
    with _Rig() as rig:
        m.UI_LANGUAGE_STATE_FILE = Path("/nonexistent-dir/x/ep.json")
        m.LANG_TRACKER = m.UiLanguageTracker()
        assert m._reassert_ui_language(object(), steps_raw=STEPS, mode="on") == "aborted_no_state"
        assert rig.clicks == []


def test_max_attempts_knob_cannot_exceed_two():
    assert m.UI_LANGUAGE_REASSERT_MAX_ATTEMPTS <= 2


if __name__ == "__main__":
    import traceback
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception:
                fails += 1
                print(f"FAIL {name}")
                traceback.print_exc()
    sys.exit(1 if fails else 0)
