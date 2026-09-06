#!/usr/bin/env python3
"""The red header banner is a second, independent fault surface.

Background (2026-09-06). The boiler raised alarm 14 ("max. Saugzeit erreicht"),
switched itself off, and sat in that state for over six hours with an empty
pellet store — and Home Assistant was never told. `alert/active` read `off` the
whole time, and it was not lying: `_handle_alert_modal` looks for a DIALOG, the
dialog for this alarm had already been dismissed, and `alert_modal` genuinely
never appeared (zero samples in the screen-identification metric).

The fault was visible the entire time on two surfaces we did not read:

    red header banner on `main`:  "14  max. Saug - Laufzeit erreicht"
    blue status strip:            "Alarm aktiv!"   (already read as status_text)

So this was a COVERAGE gap, not a matching bug. What is pinned here:

1. The banner is read, code and text together, from a real capture of the live
   alarm (tests/fixtures/screen_main_alarm_banner.png).
2. Absence is decided by COLOUR, not by whether OCR returned something — the
   same strip is grey chrome on a healthy heater, and tesseract will invent
   characters out of chrome if asked.
3. `alert/active` stays ON while a banner is up even with no dialog, and only
   starts its clear countdown when BOTH surfaces are quiet.

Run either way:
    python tests/test_alarm_banner.py     (no pytest needed; exits non-zero)
    pytest tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from PIL import Image  # noqa: E402

import main as m  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "screen_main_alarm_banner.png"


def _alarm_img() -> Image.Image:
    return Image.open(FIXTURE)


def _healthy_img() -> Image.Image:
    """The same screen with the banner painted out in header grey — what the
    panel looks like with no alarm."""
    img = _alarm_img().copy()
    grey = img.getpixel((120, 20))          # header chrome left of the band
    x, y, w, h = m.ALARM_BANNER_BBOX
    for yy in range(y, y + h):
        for xx in range(x, x + w):
            img.putpixel((xx, yy), grey)
    return img


# --------------------------------------------------------------------------
# 1. Reading the banner
# --------------------------------------------------------------------------

def test_reads_the_live_alarm_banner():
    got = m.alarm_banner_text(_alarm_img())
    assert got is not None, "the live alarm capture must produce a banner"
    # The message NUMBER is the durable identifier (14 -> manual section 5.8);
    # wording can change with firmware. A single full-width OCR pass drops it,
    # which is why the reader makes two passes.
    assert got.startswith("14 "), f"message number missing: {got!r}"
    assert "Saug" in got, f"message text missing: {got!r}"


def test_no_alarm_reads_as_none_not_as_noise():
    """The important half: grey chrome must be None, not a hallucinated string.
    Gating on OCR output alone would publish garbage on a healthy heater."""
    assert m.alarm_banner_text(_healthy_img()) is None


def test_presence_is_decided_by_colour():
    healthy = _healthy_img()
    band = m.crop(healthy, m.ALARM_BANNER_BBOX).convert("RGB")
    px = list(band.get_flattened_data())
    red = sum(1 for p in px if m._px_banner_red(p))
    assert red / len(px) < m.ALARM_BANNER_RED_FRACTION

    band = m.crop(_alarm_img(), m.ALARM_BANNER_BBOX).convert("RGB")
    px = list(band.get_flattened_data())
    red = sum(1 for p in px if m._px_banner_red(p))
    assert red / len(px) >= m.ALARM_BANNER_RED_FRACTION


def test_banner_joins_the_cycle_values():
    out = m._ocr_all({"main": _alarm_img()})
    assert "alarm_banner" in out
    assert out["alarm_banner"].startswith("14 ")
    # And it must be a discoverable HA sensor, or nothing surfaces.
    assert "alarm_banner" in m.SENSORS


def test_healthy_publishes_a_value_not_a_missing_field():
    """A None would land in neither `accepted` nor `rejected`, so the healthy
    case would count as MISSING every cycle, trip FIELD_UNAVAILABLE_AFTER_CYCLES
    and leave the alarm sensor permanently `unavailable` in HA except during a
    fault — and would make "no alarm" indistinguishable from "OCR failed"."""
    out = m._ocr_all({"main": _healthy_img()})
    assert out["alarm_banner"] == m.ALARM_BANNER_NONE
    assert out["alarm_banner"] is not None


def test_the_healthy_sentinel_does_not_read_as_an_alarm():
    """`ALARM_BANNER_NONE` is a truthy string, so the reconciler has to compare
    against it rather than rely on truthiness."""
    b = _Broker()
    for _ in range(m.ALERT_CLEAR_CONFIRM_CYCLES):
        m._reconcile_alert_state(b, False, [], m.ALARM_BANNER_NONE)
    assert _active(b) == "off", "a healthy banner must still clear the alert"


# --------------------------------------------------------------------------
# 2. Alert lifecycle: a banner alone must hold alert/active ON
# --------------------------------------------------------------------------

class _Broker:
    def __init__(self):
        self.published: dict[str, str] = {}
        self.alert_absent_streak = 0

    def publish(self, topic, payload, retain=False, **kw):
        self.published[topic] = payload

    def note_alert_seen(self):
        self.alert_absent_streak = 0

    def note_alert_absent(self):
        self.alert_absent_streak += 1
        return self.alert_absent_streak


def _active(b: _Broker):
    return b.published.get(f"{m.MQTT_TOPIC_PREFIX}/alert/active")


def test_banner_without_a_dialog_keeps_the_alert_active():
    """The exact 2026-09-06 situation: no dialog on screen, alarm very much
    real. This is the regression that matters."""
    b = _Broker()
    for _ in range(m.ALERT_CLEAR_CONFIRM_CYCLES + 3):
        m._reconcile_alert_state(b, False, [], "14 max. Saug - Laufzeit erreicht")
    assert _active(b) == "on"
    assert b.published[f"{m.MQTT_TOPIC_PREFIX}/alert/title"] == \
        "14 max. Saug - Laufzeit erreicht"
    assert b.alert_absent_streak == 0, "a live banner must not age the clear streak"


def test_clears_only_when_both_surfaces_are_quiet():
    b = _Broker()
    for _ in range(m.ALERT_CLEAR_CONFIRM_CYCLES - 1):
        m._reconcile_alert_state(b, False, [], None)
    assert _active(b) is None, "must not clear before the streak completes"
    m._reconcile_alert_state(b, False, [], None)
    assert _active(b) == "off"
    assert b.published[f"{m.MQTT_TOPIC_PREFIX}/alert/title"] == ""


def test_a_banner_resets_a_part_way_clear_countdown():
    """A fault that flickers off for one cycle must not clear the sensor."""
    b = _Broker()
    m._reconcile_alert_state(b, False, [], None)
    m._reconcile_alert_state(b, False, [], None)
    m._reconcile_alert_state(b, False, [], "14 max. Saug - Laufzeit erreicht")
    assert b.alert_absent_streak == 0
    m._reconcile_alert_state(b, False, [], None)
    assert _active(b) == "on", "streak restarted, so no clear yet"


def test_a_dialog_still_wins_and_is_left_to_the_modal_handler():
    """_handle_alert_modal already published title+body; the reconciler must
    not overwrite them with a banner-shaped message."""
    b = _Broker()
    m._reconcile_alert_state(b, False, [{"title": "T", "body": "B"}], None)
    assert b.published == {}


def test_dry_run_publishes_nothing():
    b = _Broker()
    m._reconcile_alert_state(b, True, [], "14 max. Saug - Laufzeit erreicht")
    assert b.published == {}


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
