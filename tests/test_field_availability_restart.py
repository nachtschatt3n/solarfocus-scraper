#!/usr/bin/env python3
"""A restart must not re-advertise a stale reading as live.

Background (2026-09-06). `fbh_vorlaufsolltemperatur` was reported missing every
cycle while its retained value still read 33.0 with `/available online` — i.e.
Home Assistant was rendering a number the scraper had not actually read.

The missing field itself turned out to be legitimate: in `Absenkbetrieb` the
panel does not render the Vorlaufsolltemperatur row at all (see
tests/fixtures/screen_heizkreise_og_absenkbetrieb.png, where the row is simply
absent between Vorlauftemperatur and Mischerposition). Publishing nothing for it
is correct.

The bug was the availability seeding. `_FIELD_AVAILABLE` is in-memory, so it is
empty on every pod start, and discovery published a retained "online" for every
field whose state was unknown — which is every field, every start. That
overwrote the retained "offline" of a legitimately-unavailable field while its
stale value sat on the value topic, re-advertising stale data as live for the
~3 cycles it took the streak to re-trip. With several deploys in a day, that
window kept reopening. It is also what made every restart emit a 46-field
`field_available` burst.

Pinned here: retained availability is adopted rather than assumed, and a restart
publishes no optimistic "online".

Run either way:
    python tests/test_field_availability_restart.py     (no pytest needed)
    pytest tests/
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import main as m  # noqa: E402

FIELD = "fbh_vorlaufsolltemperatur"
AVAIL = f"{m.MQTT_TOPIC_PREFIX}/{FIELD}/available"


class _Msg:
    def __init__(self, topic, payload, retain=True):
        self.topic = topic
        self.payload = payload.encode()
        self.retain = retain


class _Broker(m.MqttBroker):
    def __init__(self):
        import threading
        self._lock = threading.Lock()
        self.pending_commands = {}
        self.published = {}
        self.last_values = {}
        self.pause_state = False

    def publish(self, topic, payload, retain=False, **kw):
        self.published[topic] = payload


def _fresh_process():
    """Simulate a pod start: in-memory availability state is empty."""
    m._FIELD_AVAILABLE.clear()
    m._FIELD_NONE_STREAK.clear()


def test_retained_offline_is_adopted_on_connect():
    """The broker already knows the field was offline. Believe it."""
    _fresh_process()
    b = _Broker()
    b._on_message(None, None, _Msg(AVAIL, "offline"))
    assert m._FIELD_AVAILABLE[FIELD] is False


def test_retained_online_is_adopted_too():
    _fresh_process()
    b = _Broker()
    b._on_message(None, None, _Msg(AVAIL, "online"))
    assert m._FIELD_AVAILABLE[FIELD] is True


def test_an_adopted_offline_is_not_re_announced_as_online():
    """The regression. After adopting `offline`, nothing may flip the field back
    to online except an actual successful read."""
    _fresh_process()
    b = _Broker()
    b._on_message(None, None, _Msg(AVAIL, "offline"))
    # A publish attempt with the same state must be a no-op (transition-only).
    m._publish_field_availability(b, False, FIELD, online=False)
    assert AVAIL not in b.published


def test_a_real_read_still_brings_the_field_back():
    """Adopting offline must not strand a field that starts reading again."""
    _fresh_process()
    b = _Broker()
    b._on_message(None, None, _Msg(AVAIL, "offline"))
    m._publish_field_availability(b, False, FIELD, online=True)
    assert b.published[AVAIL] == "online"
    assert m._FIELD_AVAILABLE[FIELD] is True


def test_discovery_does_not_seed_availability():
    """The source of the bug: discovery must publish no optimistic 'online'.

    Asserted against the source because publish_discovery() needs a live broker;
    what matters is that the blanket seed is gone and stays gone."""
    src = (ROOT / "main.py").read_text(encoding="utf-8")
    assert "Availability is deliberately NOT seeded here" in src
    assert '_FIELD_AVAILABLE[field] = True\n            broker.publish(' not in src, \
        "the blanket online seed is back"


def test_only_the_field_level_available_topic_is_adopted():
    """`scraper/availability` is the whole-device LWT, not a field. Adopting it
    as a field would invent an entity named 'scraper'."""
    _fresh_process()
    b = _Broker()
    b._on_message(None, None, _Msg(f"{m.MQTT_TOPIC_PREFIX}/scraper/availability", "offline"))
    assert "scraper" not in m._FIELD_AVAILABLE


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
