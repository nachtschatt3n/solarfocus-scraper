"""Solarfocus pellet^top VNC scraper.

CLI subcommands for development + a production `run` loop. See README.md.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape as html_escape
from http.server import BaseHTTPRequestHandler, HTTPServer
from io import BytesIO
from pathlib import Path
from typing import Optional

import paho.mqtt.client as mqtt
import pytesseract
from dotenv import load_dotenv
from PIL import Image
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest
from pythonjsonlogger import jsonlogger
from vncdotool import api as vnc_api

# =============================================================================
# Calibration — measured against the real heater on 2026-04-19 (640x480 VNC).
# =============================================================================

# Tesseract config strings. Numeric whitelist + 2x LANCZOS upscale gives reliable
# reads on the small UI font.
FIELD_NUM = "--psm 7 -c tessedit_char_whitelist=0123456789.,-"
FIELD_TEXT = "--psm 7"
# Multi-line German prose inside an alert modal — psm 6 treats the crop as
# a block of text, keeping newlines in the OCR output.
FIELD_PARAGRAPH = "--psm 6"

# Bounding boxes inside the alert_modal screen. Not part of the BBOXES dict
# because they're not published as regular sensors — they fire out-of-band when
# a modal is detected, via run_cycle's _handle_alert_modal().
ALERT_TITLE_BBOX = (20, 5, 600, 28)
ALERT_BODY_BBOX  = (40, 140, 560, 180)

# Navigation as a state machine.
#
# Each `Screen` has a small distinctive region whose sha256 is the screen's
# fingerprint, plus a `parent` pointer (the screen reached via the back arrow).
# `EDGES[(src, dst)] = (x, y)` defines forward taps. `navigate_to(target)` does
# BFS over both forward edges and back edges (parent pointers), so any starting
# state can reach any target.
#
# Empty `expected_hash` means "not yet calibrated" — the screen is reachable as
# a navigation target via parent links, but `_identify_screen` won't match it.

# Top-left back arrow — used on most screens. Some screens override via Screen.back_xy.
BACK_ARROW_XY = (35, 30)

@dataclass
class Screen:
    hash_region: tuple[int, int, int, int]
    expected_hash: str
    parent: Optional[str] = None  # screen reached by clicking back_xy
    back_xy: tuple[int, int] = BACK_ARROW_XY  # override when standard back arrow isn't present

SCREENS: dict[str, Screen] = {
    "main": Screen(
        hash_region=(10, 50, 80, 18),  # "V 25.080" version text
        expected_hash="8edf4269df4e29072155260831ceb67e8adf05b8bd049a4b92bd03b144a5b62c",
        parent=None,
    ),
    "auswahlmenue": Screen(
        hash_region=(85, 5, 200, 30),  # header bar text
        expected_hash="420b8d677a88f5868bb1e18a021857ddb2ad73283ee9a57acac9a886cb08299d",
        parent="main",
    ),
    "kundenmenue": Screen(
        hash_region=(85, 5, 200, 30),
        expected_hash="1e1ff73b4be210f3bc7998a69f1b3e9605c6c0dffb0f879f7fd3ffdec26abdaf",
        parent="auswahlmenue",
    ),
    "betriebsstunden_p1": Screen(
        hash_region=(0, 80, 160, 22),  # "Saugzuggebläse" label
        expected_hash="f90b9df97fd64ed99934dfff36eccdfbe4cccb6f87b08d80582f7809e924fc70",
        parent="kundenmenue",
    ),
    "betriebsstunden_p2": Screen(
        hash_region=(0, 80, 220, 22),  # "Pelletsbetrieb Teillast" label
        expected_hash="538bd98a255a7309640989d834e15e3633f2f01c9ff029db3abadfeef9847485",
        parent="kundenmenue",
    ),
    "betriebsstunden_p3": Screen(
        hash_region=(85, 5, 470, 30),  # "Betriebsstundenzähler Wärmeverteilung" header
        expected_hash="ac5029cc4aea6d21720a014609bcbba0dd08ea78c04479b1efce838028f4b3d1",
        parent="kundenmenue",
    ),
    "kessel": Screen(
        hash_region=(10, 215, 85, 30),  # "Kessel" label at left
        expected_hash="a9d5af96364628b8634fecb7b68f8738185b9cc503f3e7230dbfc0efada7fc9a",
        parent="auswahlmenue",
    ),
    "heizkreise_og": Screen(
        hash_region=(270, 65, 80, 30),  # "OG" title text
        expected_hash="e111c25ac599314f79175b5fe68a832f68eb867dcb2e09fe08c29e1b7d01090e",
        parent="auswahlmenue",
    ),
    "warmwasser": Screen(
        hash_region=(195, 95, 290, 28),  # "Trinkwasserspeicher 1" title
        expected_hash="3e329f2a8fe5f37caa13d1f440f83af0dd8c3eb0bcc46c9f44b9a90823921279",
        parent="auswahlmenue",
    ),
    # Alert modals — Solarfocus pops these over any screen when maintenance or
    # fault conditions fire (e.g. "KESSELREINIGUNG EMPFOHLEN!", "Pellet Mangel",
    # ...). Hash region is the blue info icon centered at the top, which is the
    # same graphic across every info-type alert — so the same fingerprint covers
    # arbitrary future alert text. back_xy points at the OK button so the
    # state-machine's generic "click back to escape unknown" path dismisses the
    # modal automatically. run_cycle() additionally OCRs the body and publishes
    # it to MQTT before dismissing.
    "alert_modal": Screen(
        hash_region=(290, 50, 60, 60),  # blue info "i" icon, shared across all info alerts
        expected_hash="8168a4c150516591af7479070357376bb44c443b2efb8f1a3d00672b9dc3199e",
        parent="main",           # dismissing takes us back to underlying screen; main is a safe retry anchor
        back_xy=(320, 410),      # OK button
    ),
    # Live floor-heating heat-circuit screen. Reached via the right-arrow on
    # heizkreise_og. The standard back arrow at (35,30) is hidden behind the
    # highlighted radiator icon here, so we use the in-page left arrow at (45,130)
    # to step back to OG (which itself has the normal back arrow → auswahlmenue).
    "heizkreise_fbh": Screen(
        hash_region=(200, 65, 240, 30),  # "Fussbodenheizung" title
        expected_hash="15f80f0d9dc9c7f678813db6be2a91c53162bd7d6c5d2b7727759c135663c9f2",
        parent="heizkreise_og",
        back_xy=(45, 130),
    ),
}

# Forward edges: tap (x, y) on `src` to land on `dst`.
EDGES: dict[tuple[str, str], tuple[int, int]] = {
    ("main",               "auswahlmenue"):       (75,  15),   # date in header
    ("auswahlmenue",       "kundenmenue"):        (110, 130),  # solartop heater icon
    ("auswahlmenue",       "kessel"):             (245, 250),  # boiler tank icon
    ("auswahlmenue",       "heizkreise_og"):      (385, 130),  # radiator icon
    ("auswahlmenue",       "warmwasser"):         (525, 130),  # faucet icon (DHW tank)
    ("kundenmenue",        "betriebsstunden_p1"): (110, 265),  # COUNT button
    ("betriebsstunden_p1", "betriebsstunden_p2"): (575, 445),  # right arrow
    ("betriebsstunden_p2", "betriebsstunden_p3"): (575, 445),  # right arrow
    ("heizkreise_og",      "heizkreise_fbh"):     (565,  95),  # right arrow → next heat circuit
}

# Per field: which screen, where to crop, how to OCR, how to parse.
# `invert=True` flips the image before OCR — helps for white-on-blue (status bars)
# and white-on-grey (status text in heat-circuit screens).
@dataclass
class FieldSpec:
    screen: str
    bbox: tuple[int, int, int, int]  # x, y, w, h
    config: str                      # tesseract --psm + whitelist
    kind: str                        # "float" | "int" | "str"
    invert: bool = False

BBOXES: dict[str, FieldSpec] = {
    # main screen
    "kesseltemperatur":     FieldSpec("main", (260, 388, 100, 22), FIELD_NUM,  "float"),
    "restsauerstoffgehalt": FieldSpec("main", (260, 415, 100, 22), FIELD_NUM,  "float"),
    "outside_temperature":  FieldSpec("main", (560,  40,  35, 25), FIELD_NUM,  "float"),
    "status_text":          FieldSpec("main", (160, 448, 320, 28), FIELD_TEXT, "str", invert=True),
    # Betriebsstundenzähler page 1 (rows step ~35px starting at y=84)
    "saugzuggeblaese_h":          FieldSpec("betriebsstunden_p1", (430,  84, 100, 22), FIELD_NUM, "float"),
    "lambdasonde_h":              FieldSpec("betriebsstunden_p1", (430, 119, 100, 22), FIELD_NUM, "float"),
    "waermetauscherreinigung_h":  FieldSpec("betriebsstunden_p1", (430, 154, 100, 22), FIELD_NUM, "float"),
    "zuendung_h":                 FieldSpec("betriebsstunden_p1", (430, 189, 100, 22), FIELD_NUM, "float"),
    "einschub_h":                 FieldSpec("betriebsstunden_p1", (430, 224, 100, 22), FIELD_NUM, "float"),
    "saugaustragung_h":           FieldSpec("betriebsstunden_p1", (430, 256, 100, 26), FIELD_NUM, "float"),
    "ascheaustragungsschnecke_h": FieldSpec("betriebsstunden_p1", (430, 290, 100, 28), FIELD_NUM, "float"),
    # Betriebsstundenzähler page 2
    "pelletsbetrieb_teillast_h":      FieldSpec("betriebsstunden_p2", (430,  84,  95, 22), FIELD_NUM, "float"),
    "pelletsbetrieb_h":               FieldSpec("betriebsstunden_p2", (430, 119, 100, 22), FIELD_NUM, "float"),
    "anzahl_kesselstarts":            FieldSpec("betriebsstunden_p2", (430, 154, 100, 22), FIELD_NUM, "int"),
    "betriebsstunden_seit_wartung_h": FieldSpec("betriebsstunden_p2", (430, 189, 100, 22), FIELD_NUM, "float"),
    "pelletsverbrauch_kg":            FieldSpec("betriebsstunden_p2", (430, 224, 100, 22), FIELD_NUM, "float"),
    # Kessel screen
    "puffer_temp_top":     FieldSpec("kessel", (335, 152,  90, 25), FIELD_NUM,  "float"),
    "puffer_temp_bottom":  FieldSpec("kessel", (320, 320,  75, 30), FIELD_NUM,  "float"),
    "kessel_status_text":  FieldSpec("kessel", (100, 398, 440, 30), FIELD_TEXT, "str", invert=True),
    # Heizkreis OG screen
    # Y range covers both heat-circuit layouts: 4-row "Heizbetrieb" (value at
    # y≈325) and 3-row "Absenkbetrieb" where the Vorlaufsolltemperatur row is
    # hidden and Vorlauftemperatur shifts up to y≈320. Widened bbox captures
    # either without changing the OCR output — the digit glyphs sit centered
    # in the crop in both cases.
    "og_vorlauftemperatur":     FieldSpec("heizkreise_og", (365, 316,  80, 26), FIELD_NUM,  "float"),
    "og_vorlaufsolltemperatur": FieldSpec("heizkreise_og", (365, 355,  80, 22), FIELD_NUM,  "float"),
    "og_mischerposition":       FieldSpec("heizkreise_og", (380, 380,  70, 22), FIELD_NUM,  "float"),
    "og_status_text":           FieldSpec("heizkreise_og", (130, 410, 360, 28), FIELD_TEXT, "str", invert=True),
    "og_heizkreis_status":      FieldSpec("heizkreise_og", ( 80, 445, 520, 22), FIELD_TEXT, "str", invert=True),
    # Warmwasser (DHW) screen
    "ww_ist_temp":   FieldSpec("warmwasser", (165, 178,  90, 28), FIELD_NUM,  "float"),
    "ww_soll_temp":  FieldSpec("warmwasser", (460, 215, 140, 28), FIELD_NUM,  "float"),
    "ww_modus":      FieldSpec("warmwasser", (290, 365, 150, 25), FIELD_TEXT, "str"),
    # Betriebsstundenzähler page 3 — Wärmeverteilung (heat distribution counters)
    "rla_pumpe_h":         FieldSpec("betriebsstunden_p3", (410,  95, 140, 28), FIELD_NUM, "float"),
    "og_h":                FieldSpec("betriebsstunden_p3", (410, 135, 140, 28), FIELD_NUM, "float"),
    "fussbodenheizung_h":  FieldSpec("betriebsstunden_p3", (410, 165, 140, 28), FIELD_NUM, "float"),
    # Heizkreis Fussbodenheizung (live floor-heating circuit) — rows ~5px higher than OG
    "fbh_vorlauftemperatur":     FieldSpec("heizkreise_fbh", (365, 320, 80, 22), FIELD_NUM,  "float"),
    "fbh_vorlaufsolltemperatur": FieldSpec("heizkreise_fbh", (365, 350, 80, 24), FIELD_NUM,  "float"),
    "fbh_mischerposition":       FieldSpec("heizkreise_fbh", (365, 378, 80, 22), FIELD_NUM,  "float"),
    "fbh_status_text":           FieldSpec("heizkreise_fbh", (130, 410, 360, 28), FIELD_TEXT, "str", invert=True),
    "fbh_heizkreis_status":      FieldSpec("heizkreise_fbh", ( 80, 445, 520, 22), FIELD_TEXT, "str", invert=True),
}

# Fill-level bar interior (white when empty, dark when pellets present).
FILL_BAR_REGION: Optional[tuple[int, int, int, int]] = (545, 175, 15, 150)
FILL_BAR_FILLED_THRESHOLD = 200  # grayscale below this = filled pixel

# Sanity bounds. Status_text excluded (string).
SANITY_BOUNDS: dict[str, tuple[float, float]] = {
    "kesseltemperatur":               (-10, 150),
    "restsauerstoffgehalt":           (0,    25),
    "outside_temperature":            (-30,  50),
    "fill_level_percent":             (0,   100),
    "saugzuggeblaese_h":              (0, 1_000_000),
    "lambdasonde_h":                  (0, 1_000_000),
    "waermetauscherreinigung_h":      (0, 1_000_000),
    "zuendung_h":                     (0, 1_000_000),
    "einschub_h":                     (0, 1_000_000),
    "saugaustragung_h":               (0, 1_000_000),
    "ascheaustragungsschnecke_h":     (0, 1_000_000),
    "pelletsbetrieb_teillast_h":      (0, 1_000_000),
    "pelletsbetrieb_h":               (0, 1_000_000),
    "anzahl_kesselstarts":            (0, 1_000_000),
    "betriebsstunden_seit_wartung_h": (0, 1_000_000),
    "pelletsverbrauch_kg":            (0, 10_000_000),
    "puffer_temp_top":                (-10, 120),
    "puffer_temp_bottom":             (-10, 120),
    "og_vorlauftemperatur":           (0, 90),
    "og_vorlaufsolltemperatur":       (0, 90),
    "ww_ist_temp":                    (0, 90),
    "ww_soll_temp":                   (0, 90),
    "rla_pumpe_h":                    (0, 1_000_000),
    "og_h":                           (0, 1_000_000),
    "fussbodenheizung_h":             (0, 1_000_000),
    "fbh_vorlauftemperatur":          (0, 90),
    "fbh_vorlaufsolltemperatur":      (0, 90),
    "fbh_mischerposition":            (0, 100),
    "og_mischerposition":             (0, 100),
}

# Counter fields whose values must never decrease vs. last published value.
COUNTER_FIELDS: set[str] = {
    "saugzuggeblaese_h", "lambdasonde_h", "waermetauscherreinigung_h",
    "zuendung_h", "einschub_h", "saugaustragung_h", "ascheaustragungsschnecke_h",
    "pelletsbetrieb_teillast_h", "pelletsbetrieb_h", "anzahl_kesselstarts",
    "betriebsstunden_seit_wartung_h", "pelletsverbrauch_kg",
    "rla_pumpe_h", "og_h", "fussbodenheizung_h",
}

# Maximum absolute change allowed between consecutive cycles per field. Catches
# OCR digit-swaps / prefix-smears that would pass a static range check (e.g.
# kesseltemperatur jumping 62 → 869 — "in range" under a 0-1000 bound, but
# physically impossible at a 5-minute interval). Fields not listed here are
# not delta-checked. Tuned for the default SCRAPE_INTERVAL_SECONDS=300.
MAX_DELTA_PER_CYCLE: dict[str, float] = {
    "kesseltemperatur":            25.0,   # °C — can ramp fast when burner ignites
    "outside_temperature":          8.0,   # °C
    "puffer_temp_top":             25.0,   # °C — top of buffer heats fast under active charging
    "puffer_temp_bottom":          20.0,   # °C
    "restsauerstoffgehalt":         5.0,   # %
    "fill_level_percent":           5.0,   # % — pellet auger delivers in steps
    "og_vorlauftemperatur":         8.0,   # °C
    "og_vorlaufsolltemperatur":    20.0,   # °C — setpoint can jump on schedule
    "og_mischerposition":          30.0,   # %  — mixer can slam open/shut
    "fbh_vorlauftemperatur":        8.0,
    "fbh_vorlaufsolltemperatur":   20.0,
    "fbh_mischerposition":         30.0,
    "ww_ist_temp":                  8.0,
    "ww_soll_temp":                50.0,   # DHW setpoint flips between day/night
    # Counter fields — monotonic check already catches decreases; delta here
    # catches unreasonably large *increases* (OCR misreads that drop digits
    # or grow them, e.g. og_h jumping 36165 → 398831 on a dropped thousands
    # separator). Budget is generous: at 5-minute intervals, hour meters
    # accrue ≤0.1 h even under pure-runtime use; 2.0 lets us catch up after
    # a short VNC outage without false rejects.
    "saugzuggeblaese_h":            2.0,
    "lambdasonde_h":                2.0,
    "waermetauscherreinigung_h":    2.0,
    "zuendung_h":                   2.0,
    "einschub_h":                   2.0,
    "saugaustragung_h":             2.0,
    "ascheaustragungsschnecke_h":   2.0,
    "pelletsbetrieb_teillast_h":    2.0,
    "pelletsbetrieb_h":             2.0,
    "betriebsstunden_seit_wartung_h": 2.0,
    "rla_pumpe_h":                  2.0,
    "og_h":                         2.0,
    "fussbodenheizung_h":           2.0,
    "anzahl_kesselstarts":         10.0,   # max ~2 starts per 5min cycle; allow headroom
    "pelletsverbrauch_kg":         20.0,   # peak burn ~2 kg/min
}

# Home Assistant discovery metadata per field.
@dataclass
class SensorMeta:
    name: str  # human-readable
    unit: Optional[str] = None
    device_class: Optional[str] = None
    state_class: Optional[str] = None

SENSORS: dict[str, SensorMeta] = {
    # main screen
    "kesseltemperatur":     SensorMeta("Kesseltemperatur", "°C", "temperature", "measurement"),
    "restsauerstoffgehalt": SensorMeta("Restsauerstoffgehalt", "%", None, "measurement"),
    "status_text":          SensorMeta("Status"),
    "fill_level_percent":   SensorMeta("Pellets Füllstand", "%", None, "measurement"),
    "outside_temperature":  SensorMeta("Außentemperatur", "°C", "temperature", "measurement"),
    # Betriebsstundenzähler page 1
    "saugzuggeblaese_h":             SensorMeta("Saugzuggebläse", "h", "duration", "total_increasing"),
    "lambdasonde_h":                 SensorMeta("Lambdasonde", "h", "duration", "total_increasing"),
    "waermetauscherreinigung_h":     SensorMeta("Wärmetauscherreinigung", "h", "duration", "total_increasing"),
    "zuendung_h":                    SensorMeta("Zündung", "h", "duration", "total_increasing"),
    "einschub_h":                    SensorMeta("Einschub", "h", "duration", "total_increasing"),
    "saugaustragung_h":              SensorMeta("Saugaustragung", "h", "duration", "total_increasing"),
    "ascheaustragungsschnecke_h":    SensorMeta("Ascheaustragungsschnecke", "h", "duration", "total_increasing"),
    # Betriebsstundenzähler page 2
    "pelletsbetrieb_teillast_h":     SensorMeta("Pelletsbetrieb Teillast", "h", "duration", "total_increasing"),
    "pelletsbetrieb_h":              SensorMeta("Pelletsbetrieb", "h", "duration", "total_increasing"),
    "anzahl_kesselstarts":           SensorMeta("Kesselstarts", None, None, "total_increasing"),
    "betriebsstunden_seit_wartung_h": SensorMeta("Betriebsstunden seit Wartung", "h", "duration", "total_increasing"),
    "pelletsverbrauch_kg":           SensorMeta("Pelletsverbrauch", "kg", "weight", "total_increasing"),
    # Kessel screen
    "puffer_temp_top":               SensorMeta("Puffer oben", "°C", "temperature", "measurement"),
    "puffer_temp_bottom":            SensorMeta("Puffer unten", "°C", "temperature", "measurement"),
    "kessel_status_text":            SensorMeta("Kessel Status"),
    # Heizkreis OG
    "og_vorlauftemperatur":          SensorMeta("OG Vorlauf Ist", "°C", "temperature", "measurement"),
    "og_vorlaufsolltemperatur":      SensorMeta("OG Vorlauf Soll", "°C", "temperature", "measurement"),
    "og_mischerposition":            SensorMeta("OG Mischerposition", "%", None, "measurement"),
    "og_status_text":                SensorMeta("OG Status"),
    "og_heizkreis_status":           SensorMeta("OG Heizkreis"),
    # Warmwasser (Trinkwasserspeicher)
    "ww_ist_temp":                   SensorMeta("Warmwasser Ist", "°C", "temperature", "measurement"),
    "ww_soll_temp":                  SensorMeta("Warmwasser Soll", "°C", "temperature", "measurement"),
    "ww_modus":                      SensorMeta("Warmwasser Modus"),
    # Betriebsstundenzähler page 3 (Wärmeverteilung)
    "rla_pumpe_h":                   SensorMeta("RLA-Pumpe", "h", "duration", "total_increasing"),
    "og_h":                          SensorMeta("OG Heizkreis", "h", "duration", "total_increasing"),
    "fussbodenheizung_h":            SensorMeta("Fussbodenheizung", "h", "duration", "total_increasing"),
    # Heizkreis Fussbodenheizung (live)
    "fbh_vorlauftemperatur":         SensorMeta("FBH Vorlauf Ist", "°C", "temperature", "measurement"),
    "fbh_vorlaufsolltemperatur":     SensorMeta("FBH Vorlauf Soll", "°C", "temperature", "measurement"),
    "fbh_mischerposition":           SensorMeta("FBH Mischerposition", "%", None, "measurement"),
    "fbh_status_text":               SensorMeta("FBH Status"),
    "fbh_heizkreis_status":          SensorMeta("FBH Heizkreis"),
}

# =============================================================================
# Config
# =============================================================================

load_dotenv()

def env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.environ.get(name, default)
    if required and val is None:
        sys.exit(f"missing required env var: {name}")
    return val  # type: ignore[return-value]

VNC_HOST = env("VNC_HOST", "")
VNC_PORT = int(env("VNC_PORT", "5900"))
VNC_PASSWORD = env("VNC_PASSWORD", "")
MQTT_HOST = env("MQTT_HOST", "localhost")
MQTT_PORT = int(env("MQTT_PORT", "1883"))
MQTT_TOPIC_PREFIX = env("MQTT_TOPIC_PREFIX", "solarfocus")
MQTT_DISCOVERY_PREFIX = env("MQTT_DISCOVERY_PREFIX", "homeassistant")
MQTT_DEVICE_ID = env("MQTT_DEVICE_ID", "solarfocus_pellettop")
SCRAPE_INTERVAL_SECONDS = int(env("SCRAPE_INTERVAL_SECONDS", "300"))
VNC_CONNECT_TIMEOUT_SECONDS = int(env("VNC_CONNECT_TIMEOUT_SECONDS", "10"))
CLICK_DELAY_SECONDS = float(env("CLICK_DELAY_SECONDS", "1.5"))
METRICS_PORT = int(env("METRICS_PORT", "8080"))
LOG_LEVEL = env("LOG_LEVEL", "INFO")

SCREENSHOT_DIR = Path(__file__).parent / "screenshots"

# =============================================================================
# Logging
# =============================================================================

class _JsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record["timestamp"] = datetime.now(timezone.utc).isoformat()
        log_record["level"] = record.levelname
        log_record.setdefault("event_type", "log")

def _setup_logging() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter("%(message)s"))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(LOG_LEVEL)
    # vncdotool/twisted are noisy
    logging.getLogger("twisted").setLevel(logging.WARNING)
    logging.getLogger("vncdotool").setLevel(logging.WARNING)
    return logging.getLogger("solarfocus")

log = _setup_logging()

def event(level: int, event_type: str, message: str, **fields) -> None:
    log.log(level, message, extra={"event_type": event_type, **fields})

# =============================================================================
# Metrics
# =============================================================================

m_up = Gauge("solarfocus_scraper_up", "Process is running")
m_last_run = Gauge("solarfocus_scraper_last_run_timestamp_seconds", "Unix time of last completed cycle")
m_last_dur = Gauge("solarfocus_scraper_last_run_duration_seconds", "Duration of last cycle in seconds")
m_runs = Counter("solarfocus_scraper_runs_total", "Cycles by status", ["status"])

# =============================================================================
# VNC helpers
# =============================================================================

def _vnc_target() -> str:
    # vncdotool accepts "host::port" (double colon = explicit TCP port).
    return f"{VNC_HOST}::{VNC_PORT}"

def vnc_connect():
    """Connect with timeout. Returns the client or raises."""
    target = _vnc_target()
    event(logging.INFO, "vnc_connecting", "connecting to VNC", target=target)
    # vnc_api.connect blocks on connect; wrap with a watchdog timer if needed.
    # For now rely on TCP connect timeout being reasonable.
    client = vnc_api.connect(target, password=VNC_PASSWORD or None, timeout=VNC_CONNECT_TIMEOUT_SECONDS)
    event(logging.INFO, "vnc_connected", "VNC connected")
    return client

def vnc_capture(client, save_path: Optional[Path] = None) -> Image.Image:
    """Refresh framebuffer and return PIL Image. Optionally save PNG."""
    client.refreshScreen()
    img = client.screen.copy() if client.screen else None
    if img is None:
        raise RuntimeError("vnc client returned no screen")
    if save_path:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(save_path)
    return img

def vnc_click(client, x: int, y: int) -> None:
    client.mouseMove(x, y)
    time.sleep(0.1)
    client.mousePress(1)

# =============================================================================
# Image / OCR helpers
# =============================================================================

def crop(img: Image.Image, region: tuple[int, int, int, int]) -> Image.Image:
    x, y, w, h = region
    return img.crop((x, y, x + w, y + h))

def region_hash(img: Image.Image, region: tuple[int, int, int, int]) -> str:
    return hashlib.sha256(crop(img, region).tobytes()).hexdigest()

def ocr(img: Image.Image, region: tuple[int, int, int, int], config: str,
        lang: str = "deu", invert: bool = False) -> str:
    from PIL import ImageOps
    c = crop(img, region)
    if invert:
        # Grayscale-then-invert handles white-on-blue (status bars) much better
        # than RGB invert, which produces low-contrast yellow-on-yellow.
        c = ImageOps.invert(c.convert("L"))
    # 3x LANCZOS upscale gives tesseract larger glyphs to work with, which
    # stabilises reads across tesseract versions (the container's Debian
    # 5.3.x package and a local Arch 5.5.x can produce different results on
    # 2x-upscaled small digits — 3x closes most of that gap).
    big = c.resize((c.width * 3, c.height * 3), Image.LANCZOS)
    return pytesseract.image_to_string(big, lang=lang, config=config).strip()

def parse_value(raw: str, kind: str) -> Optional[float | int | str]:
    if kind == "str":
        return raw or None
    cleaned = raw.replace(",", ".").replace(" ", "").replace("°C", "").replace("%", "").strip()
    if not cleaned:
        return None
    try:
        if kind == "int":
            return int(cleaned)
        val = float(cleaned)
        # Dropped-decimal heuristic: Tesseract occasionally eats the '.' on
        # small fonts ("35.7" → "357", "46.9" → "469"). If we got exactly
        # 3 digits, no dot, and the result is > 200, reinterpret as "NN.N".
        # 200 was chosen as a safe threshold: real percentages top out at 100,
        # temperatures top out near 150 °C even under peak burner load, and
        # no field this scraper reads legitimately exceeds 200 as a small int.
        if "." not in cleaned and cleaned.isdigit() and len(cleaned) == 3 and val > 200:
            return float(f"{cleaned[:-1]}.{cleaned[-1]}")
        return val
    except ValueError:
        return None

def fill_level_percent(img: Image.Image) -> Optional[float]:
    if FILL_BAR_REGION is None or any(v is None for v in FILL_BAR_REGION):
        return None
    bar = crop(img, FILL_BAR_REGION).convert("L")
    pixels = list(bar.getdata())
    if not pixels:
        return None
    filled = sum(1 for p in pixels if p < FILL_BAR_FILLED_THRESHOLD)
    return round(100.0 * filled / len(pixels), 1)

# =============================================================================
# MQTT
# =============================================================================

class MqttBroker:
    def __init__(self):
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"{MQTT_DEVICE_ID}-scraper",
        )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.pause_state: bool = False  # mirrors retained solarfocus/scraper/pause
        self.last_values: dict[str, str] = {}  # field -> last retained value (string)
        self._lock = threading.Lock()

    def connect(self) -> None:
        self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self.client.loop_start()

    def disconnect(self) -> None:
        self.client.loop_stop()
        self.client.disconnect()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        event(logging.INFO, "mqtt_connected", "MQTT connected", host=MQTT_HOST, port=MQTT_PORT)
        client.subscribe(f"{MQTT_TOPIC_PREFIX}/scraper/pause")
        client.subscribe(f"{MQTT_TOPIC_PREFIX}/scraper/pause/set")
        client.subscribe(f"{MQTT_TOPIC_PREFIX}/+")  # capture retained sensor values

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode("utf-8", errors="replace")
        if topic == f"{MQTT_TOPIC_PREFIX}/scraper/pause":
            with self._lock:
                self.pause_state = payload.strip().lower() in ("on", "true", "1")
        elif topic == f"{MQTT_TOPIC_PREFIX}/scraper/pause/set":
            normalized = "on" if payload.strip().lower() in ("on", "true", "1") else "off"
            client.publish(f"{MQTT_TOPIC_PREFIX}/scraper/pause", normalized, qos=0, retain=True)
        elif topic.startswith(f"{MQTT_TOPIC_PREFIX}/") and "/" not in topic[len(MQTT_TOPIC_PREFIX) + 1:]:
            field = topic[len(MQTT_TOPIC_PREFIX) + 1:]
            with self._lock:
                self.last_values[field] = payload

    def is_paused(self) -> bool:
        with self._lock:
            return self.pause_state

    def get_last(self, field: str) -> Optional[str]:
        with self._lock:
            return self.last_values.get(field)

    def publish(self, topic: str, payload, retain: bool = False) -> None:
        if not isinstance(payload, (str, bytes)):
            payload = json.dumps(payload)
        self.client.publish(topic, payload, qos=0, retain=retain)

# =============================================================================
# HA discovery
# =============================================================================

DEVICE_BLOCK = {
    "identifiers": [MQTT_DEVICE_ID],
    "manufacturer": "Solarfocus",
    "model": "pellet^top",
    "name": "Solarfocus Pellet Heater",
}

def publish_discovery(broker: MqttBroker) -> None:
    for field, meta in SENSORS.items():
        topic = f"{MQTT_DISCOVERY_PREFIX}/sensor/{MQTT_DEVICE_ID}/{field}/config"
        cfg = {
            "name": meta.name,
            "unique_id": f"{MQTT_DEVICE_ID}_{field}",
            "object_id": f"{MQTT_DEVICE_ID}_{field}",
            "state_topic": f"{MQTT_TOPIC_PREFIX}/{field}",
            "device": DEVICE_BLOCK,
        }
        if meta.unit:
            cfg["unit_of_measurement"] = meta.unit
        if meta.device_class:
            cfg["device_class"] = meta.device_class
        if meta.state_class:
            cfg["state_class"] = meta.state_class
        broker.publish(topic, cfg, retain=True)

    # pause switch
    sw_topic = f"{MQTT_DISCOVERY_PREFIX}/switch/{MQTT_DEVICE_ID}/pause/config"
    broker.publish(sw_topic, {
        "name": "Scraper Pause",
        "unique_id": f"{MQTT_DEVICE_ID}_pause",
        "object_id": f"{MQTT_DEVICE_ID}_pause",
        "state_topic": f"{MQTT_TOPIC_PREFIX}/scraper/pause",
        "command_topic": f"{MQTT_TOPIC_PREFIX}/scraper/pause/set",
        "payload_on": "on",
        "payload_off": "off",
        "state_on": "on",
        "state_off": "off",
        "device": DEVICE_BLOCK,
    }, retain=True)

    # status sensor
    broker.publish(f"{MQTT_DISCOVERY_PREFIX}/sensor/{MQTT_DEVICE_ID}/scraper_status/config", {
        "name": "Scraper Status",
        "unique_id": f"{MQTT_DEVICE_ID}_scraper_status",
        "state_topic": f"{MQTT_TOPIC_PREFIX}/scraper/status",
        "device": DEVICE_BLOCK,
    }, retain=True)

    # Alert entities — binary_sensor for active flag, text sensors for the
    # most recent title + body (retained, so they survive a scraper restart).
    broker.publish(f"{MQTT_DISCOVERY_PREFIX}/binary_sensor/{MQTT_DEVICE_ID}/alert_active/config", {
        "name": "Alert Active",
        "unique_id": f"{MQTT_DEVICE_ID}_alert_active",
        "object_id": f"{MQTT_DEVICE_ID}_alert_active",
        "state_topic": f"{MQTT_TOPIC_PREFIX}/alert/active",
        "payload_on": "on",
        "payload_off": "off",
        "device_class": "problem",
        "device": DEVICE_BLOCK,
    }, retain=True)
    broker.publish(f"{MQTT_DISCOVERY_PREFIX}/sensor/{MQTT_DEVICE_ID}/alert_title/config", {
        "name": "Alert Title",
        "unique_id": f"{MQTT_DEVICE_ID}_alert_title",
        "object_id": f"{MQTT_DEVICE_ID}_alert_title",
        "state_topic": f"{MQTT_TOPIC_PREFIX}/alert/title",
        "device": DEVICE_BLOCK,
    }, retain=True)
    broker.publish(f"{MQTT_DISCOVERY_PREFIX}/sensor/{MQTT_DEVICE_ID}/alert_body/config", {
        "name": "Alert Body",
        "unique_id": f"{MQTT_DEVICE_ID}_alert_body",
        "object_id": f"{MQTT_DEVICE_ID}_alert_body",
        "state_topic": f"{MQTT_TOPIC_PREFIX}/alert/body",
        "device": DEVICE_BLOCK,
    }, retain=True)
    broker.publish(f"{MQTT_DISCOVERY_PREFIX}/sensor/{MQTT_DEVICE_ID}/alert_last_seen/config", {
        "name": "Alert Last Seen",
        "unique_id": f"{MQTT_DEVICE_ID}_alert_last_seen",
        "object_id": f"{MQTT_DEVICE_ID}_alert_last_seen",
        "state_topic": f"{MQTT_TOPIC_PREFIX}/alert/last_seen",
        "device_class": "timestamp",
        "device": DEVICE_BLOCK,
    }, retain=True)

# =============================================================================
# Coordinator — single owner of state machine + cycle exclusion
# =============================================================================

@dataclass
class ValueRecord:
    value: object
    ts: float  # unix time when last recorded

    def iso(self) -> str:
        return datetime.fromtimestamp(self.ts, timezone.utc).isoformat(timespec="seconds")

    def age_s(self) -> float:
        return max(0.0, time.time() - self.ts)


class Coordinator:
    """Single owner of scraper runtime state. Serializes cycles and exposes
    a thread-safe snapshot for the HTTP UI.

    Concurrency:
      - `state_lock` is held briefly for every read/write of shared state.
      - `cycle_running` is the gate: `try_begin_cycle()` returns False if a
        cycle is already in flight, so a second caller bails fast (busy).
      - VNC/OCR work happens OUTSIDE the lock; phase updates take the lock
        for milliseconds at phase boundaries.
    """
    def __init__(self):
        self.state_lock = threading.Lock()
        # Cycle status
        self.cycle_running: bool = False
        self.cycle_phase: str = "idle"          # idle|connecting|navigating|ocr|publishing|done|error
        self.cycle_phase_detail: str = ""        # eg "→ heizkreise_og"
        self.cycle_started_ts: float = 0.0
        self.cycle_completed_ts: float = 0.0
        self.last_duration_s: float = 0.0
        self.last_status: str = "(none)"         # ok|busy|navigation_failed|sanity_failed|paused|error
        self.last_error: Optional[str] = None
        # State machine snapshot
        self.current_screen: Optional[str] = None
        self.target_screen: Optional[str] = None
        self.last_screenshot_png: Optional[bytes] = None
        self.last_screenshot_ts: float = 0.0
        self.last_screenshot_screen: Optional[str] = None
        # Per-screen captures for debugging capture-timing / OCR-misread issues.
        # Indexed by screen name. Populated on every successful capture so that
        # `/screenshot/<screen>.png` can serve the latest image of that screen.
        self.per_screen_captures: dict[str, tuple[bytes, float]] = {}
        # Field values + recording timestamps
        self.values: dict[str, ValueRecord] = {}

    def try_begin_cycle(self) -> bool:
        with self.state_lock:
            if self.cycle_running:
                return False
            self.cycle_running = True
            self.cycle_phase = "connecting"
            self.cycle_phase_detail = ""
            self.cycle_started_ts = time.time()
            self.last_error = None
            return True

    def end_cycle(self, status: str, error: Optional[str] = None) -> None:
        with self.state_lock:
            self.cycle_running = False
            self.cycle_phase = "done" if status in ("ok", "paused") else "error"
            self.last_status = status
            if error is not None:
                self.last_error = error
            self.cycle_completed_ts = time.time()
            self.last_duration_s = self.cycle_completed_ts - self.cycle_started_ts

    def set_phase(self, phase: str, detail: str = "") -> None:
        with self.state_lock:
            self.cycle_phase = phase
            self.cycle_phase_detail = detail

    def set_target(self, target: Optional[str]) -> None:
        with self.state_lock:
            self.target_screen = target

    def update_after_capture(self, screen: Optional[str], img: Image.Image) -> None:
        buf = BytesIO()
        img.save(buf, format="PNG")
        png = buf.getvalue()
        now = time.time()
        with self.state_lock:
            self.current_screen = screen
            self.last_screenshot_png = png
            self.last_screenshot_ts = now
            self.last_screenshot_screen = screen
            if screen:
                self.per_screen_captures[screen] = (png, now)

    def get_screen_png(self, screen: str) -> Optional[tuple[bytes, float]]:
        with self.state_lock:
            return self.per_screen_captures.get(screen)

    def record_value(self, field: str, value: object) -> None:
        with self.state_lock:
            self.values[field] = ValueRecord(value=value, ts=time.time())

    def snapshot(self) -> dict:
        with self.state_lock:
            return {
                "cycle_running": self.cycle_running,
                "cycle_phase": self.cycle_phase,
                "cycle_phase_detail": self.cycle_phase_detail,
                "cycle_started_ts": self.cycle_started_ts,
                "cycle_completed_ts": self.cycle_completed_ts,
                "last_duration_s": self.last_duration_s,
                "last_status": self.last_status,
                "last_error": self.last_error,
                "current_screen": self.current_screen,
                "target_screen": self.target_screen,
                "last_screenshot_ts": self.last_screenshot_ts,
                "last_screenshot_screen": self.last_screenshot_screen,
                "values": dict(self.values),  # shallow copy is fine; ValueRecord is immutable enough
            }

    def get_screenshot_png(self) -> Optional[bytes]:
        with self.state_lock:
            return self.last_screenshot_png


COORD = Coordinator()


def get_last_cycle() -> float:
    """Compatibility shim for /healthz — returns most recent cycle activity."""
    snap = COORD.snapshot()
    return snap["cycle_completed_ts"] or snap["cycle_started_ts"] or 0.0


# =============================================================================
# HTTP server (metrics + healthz + status UI)
# =============================================================================

def _fmt_age(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds / 60:.0f}m{seconds % 60:.0f}s"
    return f"{seconds / 3600:.1f}h"


def _fmt_value(v: object) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:g}"
    return html_escape(str(v))


def render_status_html(snap: dict) -> str:
    now = time.time()
    cycle_age = now - snap["cycle_completed_ts"] if snap["cycle_completed_ts"] else None
    cycle_age_s = _fmt_age(cycle_age) if cycle_age is not None else "—"
    started_iso = (
        datetime.fromtimestamp(snap["cycle_completed_ts"], timezone.utc).isoformat(timespec="seconds")
        if snap["cycle_completed_ts"] else "—"
    )
    screenshot_age = (
        _fmt_age(now - snap["last_screenshot_ts"]) if snap["last_screenshot_ts"] else "—"
    )
    status_class = {
        "ok": "ok", "paused": "ok", "busy": "warn",
        "navigation_failed": "err", "sanity_failed": "err", "error": "err",
    }.get(snap["last_status"], "")
    running_badge = (
        f'<span class="badge run">running: {html_escape(snap["cycle_phase"])} '
        f'{html_escape(snap["cycle_phase_detail"])}</span>'
        if snap["cycle_running"] else ""
    )
    target_html = (
        f' → <strong>{html_escape(snap["target_screen"])}</strong>'
        if snap["target_screen"] else ""
    )
    err_html = (
        f'<div class="err-banner">⚠ last error: {html_escape(snap["last_error"])}</div>'
        if snap["last_error"] else ""
    )

    # Values table
    rows = []
    for field in sorted(snap["values"].keys()):
        rec: ValueRecord = snap["values"][field]
        meta = SENSORS.get(field)
        unit = meta.unit if meta and meta.unit else ""
        rows.append(
            f"<tr><td>{html_escape(field)}</td>"
            f"<td class='val'>{_fmt_value(rec.value)} {html_escape(unit)}</td>"
            f"<td>{rec.iso()}</td>"
            f"<td>{_fmt_age(rec.age_s())}</td></tr>"
        )
    values_table = "\n".join(rows) or "<tr><td colspan='4'><em>no values yet</em></td></tr>"

    # Screen registry
    screen_rows = []
    for name, s in SCREENS.items():
        cal = "yes" if s.expected_hash else "<span class='warn'>TODO</span>"
        bbox_count = sum(1 for spec in BBOXES.values() if spec.screen == name)
        on_now = " current" if name == snap["current_screen"] else ""
        screen_rows.append(
            f"<tr class='screen{on_now}'><td>{html_escape(name)}</td>"
            f"<td>{html_escape(s.parent or '—')}</td>"
            f"<td>{cal}</td>"
            f"<td>{bbox_count}</td></tr>"
        )

    edge_rows = "\n".join(
        f"<li>{html_escape(src)} → {html_escape(dst)} @ ({xy[0]}, {xy[1]})</li>"
        for (src, dst), xy in EDGES.items()
    )

    return f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>Solarfocus Scraper</title>
<style>
  body {{ font-family: -apple-system, system-ui, sans-serif; margin: 1em; max-width: 1200px; }}
  h1 {{ margin: 0 0 0.5em; font-size: 1.4em; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1.5em; }}
  .status-line {{ font-size: 0.95em; color: #444; margin-bottom: 1em; }}
  .badge {{ padding: 2px 8px; border-radius: 4px; font-size: 0.85em; }}
  .badge.ok  {{ background: #d4f4d4; color: #1a4d1a; }}
  .badge.warn {{ background: #fff3cd; color: #6b5400; }}
  .badge.err {{ background: #f8d7da; color: #721c24; }}
  .badge.run {{ background: #cce5ff; color: #004085; }}
  .err-banner {{ background: #f8d7da; color: #721c24; padding: 8px; border-radius: 4px; margin: 0.5em 0; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 0.85em; }}
  th, td {{ padding: 4px 8px; text-align: left; border-bottom: 1px solid #eee; }}
  th {{ background: #f5f5f5; }}
  td.val {{ font-family: ui-monospace, monospace; font-weight: 600; }}
  tr.screen.current {{ background: #e7f1ff; font-weight: 600; }}
  img.screenshot {{ max-width: 100%; border: 1px solid #ccc; border-radius: 4px; }}
  .warn {{ color: #b06a00; }}
  ul.edges {{ font-family: ui-monospace, monospace; font-size: 0.8em; padding-left: 1.5em; }}
  h2 {{ font-size: 1em; margin: 1em 0 0.4em; }}
</style></head>
<body>
<h1>Solarfocus Scraper {running_badge}</h1>
<div class="status-line">
  Last cycle: <span class="badge {status_class}">{html_escape(snap["last_status"])}</span>
  &middot; finished {started_iso} ({cycle_age_s} ago)
  &middot; took {snap["last_duration_s"]:.1f}s
  &middot; <strong>current:</strong> {html_escape(snap["current_screen"] or "—")}{target_html}
</div>
{err_html}
<div class="grid">
  <div>
    <h2>Last screenshot ({html_escape(snap["last_screenshot_screen"] or "—")} · {screenshot_age} ago)</h2>
    <img class="screenshot" src="/screenshot.png?t={snap['last_screenshot_ts']:.0f}">
  </div>
  <div>
    <h2>Values ({len(snap["values"])})</h2>
    <table>
      <tr><th>field</th><th>value</th><th>recorded (UTC)</th><th>age</th></tr>
      {values_table}
    </table>
  </div>
</div>
<h2>Screens ({len(SCREENS)})</h2>
<table>
  <tr><th>name</th><th>parent (back)</th><th>calibrated</th><th>BBOXes</th></tr>
  {''.join(screen_rows)}
</table>
<h2>Edges ({len(EDGES)})</h2>
<ul class="edges">{edge_rows}</ul>
</body></html>"""


class _HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # silence default access log
        return

    def _send(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/metrics":
            self._send(200, CONTENT_TYPE_LATEST, generate_latest())
        elif path == "/healthz":
            stale_after = SCRAPE_INTERVAL_SECONDS * 2 + 60
            age = time.time() - get_last_cycle()
            ok = age < stale_after
            self._send(200 if ok else 503, "text/plain", f"age={age:.0f}s\n".encode())
        elif path in ("/", "/status"):
            html = render_status_html(COORD.snapshot())
            self._send(200, "text/html; charset=utf-8", html.encode("utf-8"))
        elif path == "/screenshot.png":
            png = COORD.get_screenshot_png()
            if png is None:
                self._send(404, "text/plain", b"no screenshot yet\n")
            else:
                self._send(200, "image/png", png)
        elif path.startswith("/screenshot/") and path.endswith(".png"):
            # /screenshot/<screen>.png — latest capture of a named screen. Useful
            # for diagnosing per-screen OCR issues without waiting for that
            # screen to be the "last captured" one (the default /screenshot.png
            # only shows whichever screen was visited last in the cycle).
            screen = path[len("/screenshot/"):-len(".png")]
            entry = COORD.get_screen_png(screen)
            if entry is None:
                self._send(404, "text/plain",
                           f"no capture yet for screen={screen}\n".encode())
            else:
                png, _ts = entry
                self._send(200, "image/png", png)
        else:
            self._send(404, "text/plain", b"not found\n")

def start_http_server() -> None:
    srv = HTTPServer(("0.0.0.0", METRICS_PORT), _HealthHandler)
    threading.Thread(target=srv.serve_forever, daemon=True, name="http").start()
    event(logging.INFO, "http_started", "HTTP server listening", port=METRICS_PORT)

# =============================================================================
# Cycle
# =============================================================================

@dataclass
class CycleResult:
    status: str  # ok | navigation_failed | busy | sanity_failed | paused
    values: dict[str, object]
    error_image_b64: Optional[str] = None


class _NavFail(Exception):
    """Raised inside _capture_and_ocr when a screen can't be reached, so the
    outer caller can route to _handle_nav_fail without returning from deep
    nested control flow."""
    def __init__(self, screen: str):
        self.screen = screen
        super().__init__(f"could not reach {screen}")

def _identify_screen(img: Image.Image) -> Optional[str]:
    """Return the name of the matching known screen, or None."""
    for name, screen in SCREENS.items():
        if not screen.expected_hash:
            continue
        if region_hash(img, screen.hash_region) == screen.expected_hash:
            return name
    return None

def _shortest_path(start: str, end: str) -> Optional[list[str]]:
    """BFS over forward EDGES + back edges (parent pointers)."""
    if start == end:
        return [start]
    if start not in SCREENS or end not in SCREENS:
        return None
    queue: list[tuple[str, list[str]]] = [(start, [start])]
    visited = {start}
    while queue:
        node, path = queue.pop(0)
        neighbors: list[str] = [dst for (src, dst) in EDGES if src == node]
        parent = SCREENS[node].parent
        if parent:
            neighbors.append(parent)
        for n in neighbors:
            if n in visited:
                continue
            new_path = path + [n]
            if n == end:
                return new_path
            visited.add(n)
            queue.append((n, new_path))
    return None

def navigate_to(client, target: str, max_steps: int = 12) -> bool:
    """Drive VNC clicks until `_identify_screen()` returns `target`.

    Loop: detect current → BFS to target → click first edge → repeat.
    On unknown screen (None), tap back arrow to escape and retry.
    """
    if target not in SCREENS:
        event(logging.ERROR, "navigate_unknown_target", "no such screen", target=target)
        return False
    for step in range(max_steps):
        img = vnc_capture(client)
        current = _identify_screen(img)
        if current == target:
            event(logging.DEBUG, "navigate_reached", "at target", target=target, steps=step)
            return True
        if current is None:
            event(logging.INFO, "navigate_unknown_screen",
                  "unknown screen, tapping back",
                  step=step, target=target)
            vnc_click(client, *BACK_ARROW_XY)
            time.sleep(CLICK_DELAY_SECONDS)
            continue
        path = _shortest_path(current, target)
        if not path or len(path) < 2:
            event(logging.ERROR, "navigate_no_path", "no route",
                  from_=current, to=target)
            return False
        next_screen = path[1]
        if (current, next_screen) in EDGES:
            xy = EDGES[(current, next_screen)]
            edge = "forward"
        elif SCREENS[current].parent == next_screen:
            xy = SCREENS[current].back_xy
            edge = "back"
        else:
            event(logging.ERROR, "navigate_no_edge", "neither forward nor back edge",
                  from_=current, to=next_screen)
            return False
        event(logging.DEBUG, "navigate_step", "click",
              from_=current, to=next_screen, edge=edge, xy=list(xy))
        vnc_click(client, *xy)
        time.sleep(CLICK_DELAY_SECONDS)
    event(logging.ERROR, "navigate_max_steps", "exhausted steps", target=target)
    return False

def _ocr_all(img_by_screen: dict[str, Image.Image]) -> dict[str, object]:
    out: dict[str, object] = {}
    for field, spec in BBOXES.items():
        img = img_by_screen.get(spec.screen)
        if img is None:
            event(logging.WARNING, "ocr_result", "no image for screen", field=field, screen=spec.screen)
            continue
        raw = ocr(img, spec.bbox, spec.config, invert=spec.invert)
        parsed = parse_value(raw, spec.kind)
        out[field] = parsed
        event(logging.DEBUG, "ocr_result", "ocr value", field=field, raw=raw, parsed=parsed)
    if FILL_BAR_REGION is not None:
        main_img = img_by_screen.get("main")
        if main_img is not None:
            out["fill_level_percent"] = fill_level_percent(main_img)
    return out

def _handle_alert_modal(client, broker: Optional[MqttBroker], dry_run: bool,
                        max_dismissals: int = 3) -> list[dict]:
    """If the heater is displaying an alert modal, OCR the title+body, publish to
    MQTT, click OK to dismiss, and repeat — some alerts can stack (e.g. two
    maintenance reminders). Returns a list of {title, body, ts_iso} dicts, one
    per dismissal, empty if no alerts were present.
    """
    seen: list[dict] = []
    for _ in range(max_dismissals):
        img = vnc_capture(client)
        if _identify_screen(img) != "alert_modal":
            break
        title = ocr(img, ALERT_TITLE_BBOX, FIELD_TEXT, invert=True).strip()
        body = ocr(img, ALERT_BODY_BBOX, FIELD_PARAGRAPH, lang="deu").strip()
        alert = {
            "title": title,
            "body": body,
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        seen.append(alert)
        event(logging.WARNING, "alert_detected", "heater alert modal",
              title=title, body=body[:120])
        if broker and not dry_run:
            broker.publish(f"{MQTT_TOPIC_PREFIX}/alert/active", "on", retain=True)
            broker.publish(f"{MQTT_TOPIC_PREFIX}/alert/title", title, retain=True)
            broker.publish(f"{MQTT_TOPIC_PREFIX}/alert/body", body, retain=True)
            broker.publish(f"{MQTT_TOPIC_PREFIX}/alert/last_seen", alert["ts"], retain=True)
        # Dismiss via OK button (back_xy on the alert_modal screen).
        vnc_click(client, *SCREENS["alert_modal"].back_xy)
        time.sleep(CLICK_DELAY_SECONDS)
    return seen


def _sanity_check(values: dict[str, object], broker: Optional[MqttBroker]) -> Optional[str]:
    """Return error string on failure, else None.

    Three layers, cheapest first:
      1. Static bounds (SANITY_BOUNDS) — physical range per field.
      2. Counter monotonicity (COUNTER_FIELDS) — hour meters never decrease.
      3. Delta check (MAX_DELTA_PER_CYCLE) — plausible change between cycles.
         Skipped when no prior value is known (first cycle, or retained state
         was cleared) — the static bounds are the only defense then.
    """
    for field, val in values.items():
        if val is None:
            continue
        bounds = SANITY_BOUNDS.get(field)
        if bounds and isinstance(val, (int, float)):
            lo, hi = bounds
            if not (lo <= val <= hi):
                return f"{field}={val} out of bounds [{lo}, {hi}]"
        if field in COUNTER_FIELDS and broker and isinstance(val, (int, float)):
            prev_str = broker.get_last(field)
            if prev_str:
                try:
                    prev = float(prev_str)
                    if val < prev:
                        return f"{field}={val} decreased from {prev}"
                except ValueError:
                    pass
        max_delta = MAX_DELTA_PER_CYCLE.get(field)
        if max_delta is not None and broker and isinstance(val, (int, float)):
            prev_str = broker.get_last(field)
            if prev_str:
                try:
                    prev = float(prev_str)
                    if abs(val - prev) > max_delta:
                        return (f"{field}={val} delta={val - prev:+.1f} "
                                f"exceeds ±{max_delta} from prev={prev} "
                                "(likely OCR misread)")
                except ValueError:
                    pass
    return None

def run_cycle(broker: Optional[MqttBroker], dry_run: bool = False, first_run_ref: Optional[list[bool]] = None) -> CycleResult:
    """One full cycle, gated by COORD.try_begin_cycle().

    A second concurrent caller returns CycleResult(status='busy') immediately
    rather than racing the in-flight cycle. broker=None for pure dry-run.
    """
    if not COORD.try_begin_cycle():
        event(logging.WARNING, "cycle_busy_skip", "another cycle already running, skipping")
        m_runs.labels(status="busy").inc()
        return CycleResult(status="busy", values={})

    event(logging.INFO, "cycle_start", "cycle starting")
    final_status = "error"
    final_error: Optional[str] = None
    try:
        if broker and broker.is_paused():
            event(logging.INFO, "paused", "scraper paused via MQTT toggle")
            m_runs.labels(status="paused").inc()
            if not dry_run:
                broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/status", "paused", retain=True)
            final_status = "paused"
            return CycleResult(status="paused", values={})

        try:
            client = vnc_connect()
        except Exception as e:
            event(logging.WARNING, "vnc_connect_failed", "VNC connect failed (likely busy)", error=str(e))
            m_runs.labels(status="busy").inc()
            if broker and not dry_run:
                broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/status", "busy", retain=True)
            final_status, final_error = "busy", str(e)
            return CycleResult(status="busy", values={})

        try:
            # First, dismiss any alert modal(s) currently on screen. These pop
            # over any screen and block navigation until OK is clicked. The
            # helper publishes each one to MQTT before dismissing.
            COORD.set_phase("alerts")
            alerts = _handle_alert_modal(client, broker, dry_run)
            if broker and not dry_run and not alerts:
                # No alert this cycle — clear the retained active flag so HA
                # reflects the current state. Title/body are left retained so
                # the last alert's text persists as reference.
                broker.publish(f"{MQTT_TOPIC_PREFIX}/alert/active", "off", retain=True)

            screens_needed = sorted({spec.screen for spec in BBOXES.values()} | {"main"})

            def _capture_and_ocr() -> dict[str, object]:
                """Navigate each needed screen, capture, OCR everything, record in COORD."""
                img_by_screen: dict[str, Image.Image] = {}
                for screen_name in screens_needed:
                    COORD.set_phase("navigating", detail=f"→ {screen_name}")
                    COORD.set_target(screen_name)
                    if not navigate_to(client, screen_name):
                        raise _NavFail(screen_name)
                    # Extra settle time before capturing for OCR. navigate_to
                    # returns as soon as the header hash matches, which can
                    # fire 1-2 frames before the rest of the screen (counter
                    # value rows, status bars) has finished drawing in —
                    # partial captures made og_h read 31 (real 36177) and
                    # fussbodenheizung_h read 398831 (real 39881) every cycle
                    # while rla_pumpe_h (top row) read correctly. 2s is the
                    # shortest delay that consistently gave full redraws in
                    # testing; total cycle adds ~16s.
                    time.sleep(2.0)
                    img = vnc_capture(client)
                    img_by_screen[screen_name] = img
                    COORD.update_after_capture(screen_name, img)
                COORD.set_phase("ocr")
                COORD.set_target(None)
                v = _ocr_all(img_by_screen)
                for field, val in v.items():
                    COORD.record_value(field, val)
                return v

            try:
                values = _capture_and_ocr()
            except _NavFail as nf:
                final_status = "navigation_failed"
                final_error = f"could not reach {nf.screen}"
                return _handle_nav_fail(client, broker, dry_run, nf.screen)

            # First sanity pass. If it trips, re-capture + re-OCR everything
            # once — the heater's UI occasionally redraws mid-frame and a
            # slightly different capture resolves the OCR misread. This keeps
            # the scraper unstuck after transient OCR spills (e.g. '35' read
            # as '357' when the bbox overlaps a row boundary) without the
            # complexity of partial publishes.
            err = _sanity_check(values, broker)
            if err:
                event(logging.WARNING, "sanity_retry",
                      "sanity failed on first pass, re-capturing + re-OCR'ing",
                      reason=err)
                try:
                    values_retry = _capture_and_ocr()
                except _NavFail as nf:
                    final_status = "navigation_failed"
                    final_error = f"could not reach {nf.screen} (on retry)"
                    return _handle_nav_fail(client, broker, dry_run, nf.screen)
                values = values_retry
                err = _sanity_check(values, broker)
        finally:
            try:
                client.disconnect()
            except Exception:
                pass

        if err:
            event(logging.ERROR, "sanity_check_failed",
                  "sanity failed after retry", reason=err)
            m_runs.labels(status="sanity_failed").inc()
            if broker and not dry_run:
                broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/status", "sanity_failed", retain=True)
            final_status, final_error = "sanity_failed", err
            return CycleResult(status="sanity_failed", values=values)

        if broker and not dry_run:
            COORD.set_phase("publishing")
            if first_run_ref and first_run_ref[0]:
                publish_discovery(broker)
                first_run_ref[0] = False
            for field, val in values.items():
                if val is None:
                    continue
                # Retain sensor values so HA recovers state on restart and the
                # delta check in _sanity_check has a stable baseline after a
                # pod restart. Non-retained publishes were dropping HA entities
                # to "unknown" after every HA reload.
                broker.publish(f"{MQTT_TOPIC_PREFIX}/{field}", str(val), retain=True)
                event(logging.DEBUG, "mqtt_published", "value published", field=field, value=val)
            broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/status", "ok", retain=True)
            broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/last_run",
                           datetime.now(timezone.utc).isoformat(), retain=True)

        duration = time.time() - COORD.cycle_started_ts
        m_last_run.set(time.time())
        m_last_dur.set(duration)
        m_runs.labels(status="ok").inc()
        final_status = "ok"
        event(logging.INFO, "cycle_complete", "cycle ok", duration_s=round(duration, 2),
              field_count=len([v for v in values.values() if v is not None]))
        return CycleResult(status="ok", values=values)
    except Exception as e:
        final_status, final_error = "error", str(e)
        event(logging.ERROR, "cycle_error", "unhandled cycle exception", error=str(e))
        raise
    finally:
        COORD.end_cycle(final_status, error=final_error)

def _handle_nav_fail(client, broker: Optional[MqttBroker], dry_run: bool, screen: str) -> CycleResult:
    try:
        img = vnc_capture(client)
        buf = BytesIO()
        img.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode()
    except Exception:
        b64 = None
    m_runs.labels(status="navigation_failed").inc()
    if broker and not dry_run and b64:
        broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/last_error_image", b64, retain=True)
        broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/status", "navigation_failed", retain=True)
    return CycleResult(status="navigation_failed", values={}, error_image_b64=b64)

# =============================================================================
# CLI subcommands
# =============================================================================

def _ts() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")

def cmd_probe(args) -> None:
    client = vnc_connect()
    try:
        path = SCREENSHOT_DIR / f"probe-{_ts()}.png"
        vnc_capture(client, save_path=path)
        print(f"saved {path}")
    finally:
        client.disconnect()

def cmd_click(args) -> None:
    client = vnc_connect()
    try:
        vnc_click(client, args.x, args.y)
        time.sleep(CLICK_DELAY_SECONDS)
        path = SCREENSHOT_DIR / f"click-{args.x}x{args.y}-{_ts()}.png"
        vnc_capture(client, save_path=path)
        print(f"clicked ({args.x},{args.y}); saved {path}")
    finally:
        client.disconnect()

def cmd_explore(args) -> None:
    client = vnc_connect()
    try:
        path = SCREENSHOT_DIR / f"explore-00-{_ts()}.png"
        vnc_capture(client, save_path=path)
        print(f"start: {path}")
        i = 1
        while True:
            line = input("next click 'x y label' (q to quit): ").strip()
            if line.lower() in ("q", "quit", "exit"):
                break
            parts = line.split(maxsplit=2)
            if len(parts) < 2:
                print("usage: <x> <y> [label]")
                continue
            try:
                x, y = int(parts[0]), int(parts[1])
            except ValueError:
                print("x and y must be integers")
                continue
            label = parts[2] if len(parts) == 3 else f"step{i}"
            vnc_click(client, x, y)
            time.sleep(CLICK_DELAY_SECONDS)
            path = SCREENSHOT_DIR / f"explore-{i:02d}-{label.replace(' ', '_')}-{_ts()}.png"
            vnc_capture(client, save_path=path)
            print(f"  → {path}")
            i += 1
    finally:
        client.disconnect()

def _parse_region(s: str) -> tuple[int, int, int, int]:
    parts = [int(p.strip()) for p in s.split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("region must be x,y,w,h")
    return tuple(parts)  # type: ignore[return-value]

def cmd_ocr(args) -> None:
    img = Image.open(args.image)
    raw = ocr(img, args.region, args.psm, lang=args.lang, invert=args.invert)
    print(f"raw: {raw!r}")
    for kind in ("float", "int"):
        try:
            print(f"as {kind}: {parse_value(raw, kind)}")
        except Exception as e:
            print(f"as {kind}: error {e}")

def cmd_hash(args) -> None:
    img = Image.open(args.image)
    print(region_hash(img, args.region))

def cmd_cycle(args) -> None:
    broker: Optional[MqttBroker] = None
    if not args.no_mqtt:
        broker = MqttBroker()
        broker.connect()
        time.sleep(2)  # let retained messages arrive
    try:
        result = run_cycle(broker, dry_run=args.dry_run, first_run_ref=[True])
        print(json.dumps({"status": result.status, "values": result.values}, indent=2, default=str))
    finally:
        if broker:
            broker.disconnect()

def cmd_navigate(args) -> None:
    """Navigate to a named screen, capture the result."""
    client = vnc_connect()
    try:
        ok = navigate_to(client, args.screen)
        path = SCREENSHOT_DIR / f"navigate-{args.screen}-{_ts()}.png"
        vnc_capture(client, save_path=path)
        print(f"{'ok' if ok else 'FAILED'}; saved {path}")
    finally:
        client.disconnect()

def cmd_screens(args) -> None:
    """List configured screens and their calibration status."""
    print(f"{'name':25s} {'parent':20s} status")
    for name, s in SCREENS.items():
        status = "calibrated" if s.expected_hash else "TODO"
        print(f"  {name:25s} {(s.parent or '-'):20s} {status}")
    print()
    print(f"{len(EDGES)} forward edges:")
    for (src, dst), xy in EDGES.items():
        print(f"  {src:22s} → {dst:22s} @ {xy}")

def cmd_calibrate(args) -> None:
    """Capture current screen, hash the configured region, write to SCREENS[name].expected_hash.

    Use after navigating to a screen via `navigate` or `explore`. Edits main.py in-place.
    """
    client = vnc_connect()
    try:
        img = vnc_capture(client)
    finally:
        client.disconnect()

    name = args.screen
    if name not in SCREENS:
        sys.exit(f"unknown screen: {name}. Add it to SCREENS first.")
    region = SCREENS[name].hash_region
    h = region_hash(img, region)
    print(f"screen={name} region={region} hash={h}")
    save_path = SCREENSHOT_DIR / f"calibrate-{name}-{_ts()}.png"
    img.save(save_path)
    print(f"saved {save_path}")
    print()
    print("Paste this into SCREENS[\"%s\"].expected_hash:" % name)
    print(f'    expected_hash="{h}",')

def cmd_run(args) -> None:
    if not VNC_HOST:
        sys.exit("VNC_HOST not set")
    m_up.set(1)
    # Seed cycle_completed_ts so /healthz doesn't 503 before the first cycle finishes.
    with COORD.state_lock:
        COORD.cycle_completed_ts = time.time()
    start_http_server()
    broker: Optional[MqttBroker] = None
    if not args.no_mqtt:
        broker = MqttBroker()
        broker.connect()
        time.sleep(2)
    first_run_ref = [True]
    event(logging.INFO, "startup", "scraper started",
          interval_s=SCRAPE_INTERVAL_SECONDS, mqtt=not args.no_mqtt)
    try:
        while True:
            try:
                run_cycle(broker, dry_run=args.no_mqtt, first_run_ref=first_run_ref)
            except Exception as e:
                event(logging.ERROR, "cycle_error", "unhandled cycle exception", error=str(e))
            time.sleep(args.interval if args.interval else SCRAPE_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        event(logging.INFO, "shutdown", "interrupted")
    finally:
        if broker:
            broker.disconnect()

def main() -> None:
    p = argparse.ArgumentParser(prog="solarfocus-scraper")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("probe", help="connect VNC and capture main screen").set_defaults(func=cmd_probe)

    pc = sub.add_parser("click", help="click at (x,y), capture result")
    pc.add_argument("x", type=int)
    pc.add_argument("y", type=int)
    pc.set_defaults(func=cmd_click)

    sub.add_parser("explore", help="interactive click-and-capture loop").set_defaults(func=cmd_explore)

    po = sub.add_parser("ocr", help="OCR a region of an image file")
    po.add_argument("image")
    po.add_argument("region", type=_parse_region, help="x,y,w,h")
    po.add_argument("--psm", default=FIELD_NUM, help=f"tesseract config (default: {FIELD_NUM})")
    po.add_argument("--lang", default="deu")
    po.add_argument("--invert", action="store_true", help="invert image before OCR (helps for white-on-dark)")
    po.set_defaults(func=cmd_ocr)

    ph = sub.add_parser("hash", help="sha256 of an image region")
    ph.add_argument("image")
    ph.add_argument("region", type=_parse_region, help="x,y,w,h")
    ph.set_defaults(func=cmd_hash)

    pcy = sub.add_parser("cycle", help="run one full cycle")
    pcy.add_argument("--dry-run", action="store_true", help="don't publish to MQTT")
    pcy.add_argument("--no-mqtt", action="store_true", help="don't connect to MQTT at all")
    pcy.set_defaults(func=cmd_cycle)

    pn = sub.add_parser("navigate", help="state-machine: drive VNC to a named screen")
    pn.add_argument("screen", help=f"one of: {', '.join(SCREENS.keys())}")
    pn.set_defaults(func=cmd_navigate)

    sub.add_parser("screens", help="list configured screens + edges").set_defaults(func=cmd_screens)

    pcal = sub.add_parser("calibrate", help="capture current screen, print hash for paste")
    pcal.add_argument("screen", help="screen name (must already exist in SCREENS)")
    pcal.set_defaults(func=cmd_calibrate)

    pr = sub.add_parser("run", help="production loop")
    pr.add_argument("--no-mqtt", action="store_true", help="dev mode: skip MQTT, just status UI + cycles")
    pr.add_argument("--interval", type=int, default=0, help="override SCRAPE_INTERVAL_SECONDS")
    pr.set_defaults(func=cmd_run)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
