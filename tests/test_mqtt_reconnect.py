"""MQTT initial-connect resilience tests.

Guards the retry-with-backoff on the MQTT connect path. Before this, a bare
`client.connect()` at startup raised ConnectionRefusedError whenever the broker
was momentarily down (node reboot / mosquitto restart), the process exited 1,
and k8s CrashLoopBackOffs it (paged CRITICAL for a transient blip). The broker
must instead *wait* for Mosquitto to come back.

No real broker/socket: MqttBroker's paho client is swapped for a fake whose
connect() refuses N times then succeeds, and sleep is stubbed so the exponential
backoff is exercised without real waiting.

Run: `python tests/test_mqtt_reconnect.py` (exits non-zero on failure) or `pytest`.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import main as m  # noqa: E402


class _FakeClient:
    """Stand-in for paho's Client: connect() refuses `fail_times` times then OKs."""

    def __init__(self, fail_times: int, exc: Exception | None = None):
        self.fail_times = fail_times
        self.exc = exc or ConnectionRefusedError(111, "Connection refused")
        self.connect_calls = 0
        self.reconnect_delay_args: tuple | None = None
        self.loop_started = False
        self.will: tuple | None = None   # LWT marks heater sensors unavailable

    def will_set(self, topic, payload=None, qos=0, retain=False):
        self.will = (topic, payload, qos, retain)

    def connect(self, host, port, keepalive=60):
        self.connect_calls += 1
        if self.connect_calls <= self.fail_times:
            raise self.exc

    def reconnect_delay_set(self, min_delay, max_delay):
        self.reconnect_delay_args = (min_delay, max_delay)

    def loop_start(self):
        self.loop_started = True


def _broker_with(fake: _FakeClient) -> m.MqttBroker:
    b = m.MqttBroker()  # constructs a real paho client (no I/O) ...
    b.client = fake     # ... which we replace before any connect happens
    return b


def test_connect_retries_until_broker_accepts():
    """Three refusals then success: connect() must not raise, and must have
    retried exactly the right number of times."""
    fake = _FakeClient(fail_times=3)
    b = _broker_with(fake)
    slept: list[float] = []
    b._connect_with_backoff(sleep=slept.append)
    assert fake.connect_calls == 4, f"expected 4 connect attempts, got {fake.connect_calls}"
    assert len(slept) == 3, f"expected 3 backoff sleeps, got {len(slept)}"


def test_backoff_is_exponential_and_capped():
    """Delays double each attempt and never exceed the configured cap."""
    fake = _FakeClient(fail_times=12)
    b = _broker_with(fake)
    slept: list[float] = []
    b._connect_with_backoff(sleep=slept.append)

    # Exponential from the initial delay: 1, 2, 4, 8, ...
    init = m.MQTT_CONNECT_BACKOFF_INITIAL_SECONDS
    assert slept[0] == init
    assert slept[1] == init * 2
    assert slept[2] == init * 4
    # Monotonically non-decreasing, and hard-capped.
    for prev, cur in zip(slept, slept[1:]):
        assert cur >= prev
    assert max(slept) <= m.MQTT_CONNECT_BACKOFF_MAX_SECONDS
    # Late attempts sit exactly at the cap (12 failures blow past it).
    assert slept[-1] == m.MQTT_CONNECT_BACKOFF_MAX_SECONDS


def test_first_attempt_success_does_not_sleep():
    """Healthy broker: connect on the first try, zero backoff sleeps."""
    fake = _FakeClient(fail_times=0)
    b = _broker_with(fake)
    slept: list[float] = []
    b._connect_with_backoff(sleep=slept.append)
    assert fake.connect_calls == 1
    assert slept == []


def test_connect_sets_reconnect_backoff_and_starts_loop():
    """Full connect() path wires paho's auto-reconnect cap and starts the loop
    so mid-run broker drops recover without killing the process. Healthy broker
    here so the (def-time-bound) real sleep is never reached."""
    fake = _FakeClient(fail_times=0)
    b = _broker_with(fake)
    b.connect()
    assert fake.reconnect_delay_args == (
        int(m.MQTT_CONNECT_BACKOFF_INITIAL_SECONDS),
        int(m.MQTT_CONNECT_BACKOFF_MAX_SECONDS),
    )
    assert fake.loop_started is True


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
