"""MQTT auto-discovery regression tests.

Guards the health/diagnostic entities the dashboard relies on — in particular
the `image.` entity for the last-error framebuffer (added so ha-agent's
picture-card is turnkey). No broker needed: publish_discovery is driven with a
stub that records every (topic, payload, retain).

Run: `python tests/test_discovery.py` (exits non-zero on failure) or `pytest`.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import main as m  # noqa: E402


class _StubBroker:
    def __init__(self):
        self.emitted: dict[str, tuple[object, bool]] = {}

    def publish(self, topic, payload, retain=False):
        self.emitted[topic] = (payload, retain)


def _discover() -> dict[str, tuple[object, bool]]:
    b = _StubBroker()
    m.publish_discovery(b)
    return b.emitted


def test_last_error_image_entity_discovered():
    """The last_error_image is exposed as a retained MQTT `image` entity so a
    dashboard can show the frame the scraper got stuck on."""
    emitted = _discover()
    key = f"{m.MQTT_DISCOVERY_PREFIX}/image/{m.MQTT_DEVICE_ID}/last_error_image/config"
    assert key in emitted, "image discovery config not published"
    payload, retain = emitted[key]
    assert retain is True, "discovery must be retained"
    # Moved off the sensor wildcard tree 2026-09-01: retained, but on the
    # sibling diag prefix so it stops dominating a `solarfocus/#` dump.
    assert payload["image_topic"] == f"{m.MQTT_DIAG_TOPIC_PREFIX}/scraper/last_error_image"
    assert payload["image_encoding"] == "b64"
    assert payload["content_type"] == "image/png"
    assert payload["unique_id"] == f"{m.MQTT_DEVICE_ID}_last_error_image"


def test_core_health_entities_still_discovered():
    """Regression: the health entities the dashboard design depends on are all
    still published (status, last_run, alert_active)."""
    emitted = _discover()
    dp, dev = m.MQTT_DISCOVERY_PREFIX, m.MQTT_DEVICE_ID
    for key in (
        f"{dp}/sensor/{dev}/scraper_status/config",
        f"{dp}/sensor/{dev}/scraper_last_run/config",
        f"{dp}/binary_sensor/{dev}/alert_active/config",
    ):
        assert key in emitted, f"missing discovery: {key}"


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
