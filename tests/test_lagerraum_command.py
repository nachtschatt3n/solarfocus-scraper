#!/usr/bin/env python3
"""The one control we press on the heater — and everything that stops us.

"Lagerraum befüllt" tells the boiler its pellet store has been refilled. It is
the only state-changing control the scraper touches, and it is only ever
touched because a human pressed a button in Home Assistant.

Getting it wrong is not cosmetic. Saying "the store is full" when it is empty
sends the auger back into an empty room, which is exactly how alarm 14
("max. Saugzeit erreicht") is raised — the alarm that switched the boiler off
for six hours on 2026-09-06.

Pinned here:

1. Commands execute on the CYCLE, never on the MQTT callback thread. VNC is
   serialised by COORD.try_begin_cycle(); clicking from a paho callback would
   race the in-flight cycle.
2. One press = at most one click. The queue entry is consumed whether or not it
   is acted on.
3. A RETAINED command is a replay, not an instruction, and is ignored — a
   broker restart must not press buttons on a heater.
4. A stale command is dropped rather than executed late.
5. If every probe still reads empty, the press is REFUSED, because the operator
   almost certainly pressed before the delivery arrived.
6. Refusals are published, so a swallowed press is never silent.

Run either way:
    python tests/test_lagerraum_command.py     (no pytest needed; exits non-zero)
    pytest tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from PIL import Image  # noqa: E402

import main as m  # noqa: E402

CMD = "lagerraum_befuellt"
SET_TOPIC = f"{m.MQTT_TOPIC_PREFIX}/command/{CMD}/set"
RESULT_TOPIC = f"{m.MQTT_TOPIC_PREFIX}/command/{CMD}/result"


class _Msg:
    def __init__(self, topic, payload=b"press", retain=False):
        self.topic = topic
        self.payload = payload
        self.retain = retain


class _Broker(m.MqttBroker):
    """Real command bookkeeping, no network."""

    def __init__(self):
        import threading
        self._lock = threading.Lock()
        self.pending_commands = {}
        self.published = {}

    def publish(self, topic, payload, retain=False, **kw):
        self.published[topic] = payload


def _probe_img(full: bool) -> Image.Image:
    """Synthesise the probe screen with all dots green (full) or red (empty)."""
    img = Image.new("RGB", (640, 480), (200, 200, 200))
    colour = (0, 200, 0) if full else (200, 0, 0)
    for x, y, w, h in m.PROBE_DOT_REGIONS.values():
        for yy in range(y, y + h):
            for xx in range(x, x + w):
                img.putpixel((xx, yy), colour)
    return img


class _Client:
    def __init__(self):
        self.clicks = []


def _fake_click(client, x, y):
    client.clicks.append((x, y))


# --------------------------------------------------------------------------
# 1. Queueing
# --------------------------------------------------------------------------

def test_a_press_queues_a_command():
    b = _Broker()
    b._on_message(None, None, _Msg(SET_TOPIC))
    assert CMD in b.pending_commands


def test_a_retained_command_is_ignored():
    """A retained command is a replay. A broker restart must not press this."""
    b = _Broker()
    b._on_message(None, None, _Msg(SET_TOPIC, retain=True))
    assert b.pending_commands == {}


def test_a_command_is_claimed_exactly_once():
    b = _Broker()
    b._on_message(None, None, _Msg(SET_TOPIC))
    assert b.take_pending_command(CMD) is True
    assert b.take_pending_command(CMD) is False


def test_a_stale_command_is_dropped_not_executed_late():
    b = _Broker()
    b._on_message(None, None, _Msg(SET_TOPIC))
    b.pending_commands[CMD] -= (m.COMMAND_MAX_AGE_SECONDS + 1)
    assert b.take_pending_command(CMD) is False
    assert b.pending_commands == {}


def test_no_command_means_no_claim():
    assert _Broker().take_pending_command(CMD) is False


# --------------------------------------------------------------------------
# 2. Execution and the empty-store guard
# --------------------------------------------------------------------------

def test_press_goes_through_when_probes_show_pellets(monkeypatch=None):
    b = _Broker()
    c = _Client()
    orig, m.vnc_click = m.vnc_click, _fake_click
    try:
        b._on_message(None, None, _Msg(SET_TOPIC))
        m._maybe_press_lagerraum_befuellt(c, b, False, _probe_img(full=True))
    finally:
        m.vnc_click = orig
    assert c.clicks == [m.LAGERRAUM_BEFUELLT_XY]
    assert b.published[RESULT_TOPIC].startswith("ok:")


def test_press_is_refused_when_every_probe_is_empty():
    """The damaging case: pressing before the delivery arrives would tell the
    boiler it has fuel and send the auger into an empty room."""
    b = _Broker()
    c = _Client()
    orig, m.vnc_click = m.vnc_click, _fake_click
    try:
        b._on_message(None, None, _Msg(SET_TOPIC))
        m._maybe_press_lagerraum_befuellt(c, b, False, _probe_img(full=False))
    finally:
        m.vnc_click = orig
    assert c.clicks == [], "must not click while the store still reads empty"
    assert b.published[RESULT_TOPIC].startswith("refused:")
    assert "empty" in b.published[RESULT_TOPIC]


def test_nothing_happens_without_a_command():
    """The cycle passes this screen every time. It must be inert by default —
    this is the property that keeps an autonomous press impossible."""
    b = _Broker()
    c = _Client()
    orig, m.vnc_click = m.vnc_click, _fake_click
    try:
        m._maybe_press_lagerraum_befuellt(c, b, False, _probe_img(full=True))
    finally:
        m.vnc_click = orig
    assert c.clicks == []
    assert b.published == {}


def test_dry_run_never_clicks():
    b = _Broker()
    c = _Client()
    orig, m.vnc_click = m.vnc_click, _fake_click
    try:
        b._on_message(None, None, _Msg(SET_TOPIC))
        m._maybe_press_lagerraum_befuellt(c, b, True, _probe_img(full=True))
    finally:
        m.vnc_click = orig
    assert c.clicks == []


def test_a_refused_press_is_still_consumed():
    """Otherwise a refusal would retry every cycle for as long as the command
    sat in the queue, against a panel whose state has moved on."""
    b = _Broker()
    c = _Client()
    orig, m.vnc_click = m.vnc_click, _fake_click
    try:
        b._on_message(None, None, _Msg(SET_TOPIC))
        m._maybe_press_lagerraum_befuellt(c, b, False, _probe_img(full=False))
        m._maybe_press_lagerraum_befuellt(c, b, False, _probe_img(full=True))
    finally:
        m.vnc_click = orig
    assert c.clicks == [], "the consumed command must not resurrect"


def test_click_target_is_the_button_not_the_acknowledge_control():
    """Regression against the confusion this whole feature grew out of: the
    acknowledge control lives in the alert DIALOG, and is never ours to press."""
    assert m.LAGERRAUM_BEFUELLT_XY == (109, 274)
    assert m.LAGERRAUM_BEFUELLT_SCREEN == "automatische_saugsondenumschalteinheit"


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
