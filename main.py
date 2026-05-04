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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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
# Short single-word button captions ("Aus"/"Ein", "AUTO"/"MAN"). psm 8 prevents
# tesseract from hallucinating trailing "B"/"." artifacts out of the button's
# rounded-rectangle border, which psm 7 tokenizes as an extra word.
FIELD_WORD = "--psm 8"
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
    # OCR fallback used when `expected_hash` doesn't match. `_identify_screen`
    # OCRs `ocr_region` (defaulting to `hash_region`) and accepts the screen if
    # `ocr_text` appears as a case-insensitive substring of the decoded string.
    # This survives VNC compression jitter, firmware UI tweaks, and other
    # pixel-level noise that silently invalidated the hash. Leave ocr_text=None
    # to opt a screen out of OCR fallback (e.g. icon-only modals).
    ocr_region: Optional[tuple[int, int, int, int]] = None
    ocr_text: Optional[str] = None

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
        ocr_text="Auswahl",  # "Auswahlmenü" — umlaut-tolerant substring
    ),
    "kundenmenue": Screen(
        hash_region=(85, 5, 200, 30),
        expected_hash="1e1ff73b4be210f3bc7998a69f1b3e9605c6c0dffb0f879f7fd3ffdec26abdaf",
        parent="auswahlmenue",
        ocr_text="Kunden",  # "Kundenmenü"
    ),
    "betriebsstunden_p1": Screen(
        hash_region=(0, 80, 160, 22),  # "Saugzuggebläse" label
        expected_hash="f90b9df97fd64ed99934dfff36eccdfbe4cccb6f87b08d80582f7809e924fc70",
        parent="kundenmenue",
        ocr_text="Saugzug",
    ),
    "betriebsstunden_p2": Screen(
        hash_region=(0, 80, 220, 22),  # "Pelletsbetrieb Teillast" label
        expected_hash="538bd98a255a7309640989d834e15e3633f2f01c9ff029db3abadfeef9847485",
        parent="kundenmenue",
        ocr_text="Pelletsbetrieb",
    ),
    "betriebsstunden_p3": Screen(
        hash_region=(85, 5, 470, 30),  # "Betriebsstundenzähler Wärmeverteilung" header
        expected_hash="ac5029cc4aea6d21720a014609bcbba0dd08ea78c04479b1efce838028f4b3d1",
        parent="kundenmenue",
        # Persistent alert banners ("Pelletsmangel im Lagerraum!" etc.) can
        # overwrite the header text, which breaks the hash and makes the
        # old header-based ocr_text ("rmeverteilung") un-findable. Move OCR
        # fallback into the body — the "RLA Pumpe" label at (~0,95) is the
        # top row of p3's heat-distribution table and unique to this page.
        ocr_region=(0, 95, 180, 28),
        ocr_text="RLA",
    ),
    "kessel": Screen(
        hash_region=(10, 215, 85, 30),  # "Kessel" label at left
        expected_hash="a9d5af96364628b8634fecb7b68f8738185b9cc503f3e7230dbfc0efada7fc9a",
        parent="auswahlmenue",
        ocr_text="Kessel",
    ),
    "heizkreise_og": Screen(
        hash_region=(270, 65, 80, 30),  # "OG" title text
        expected_hash="e111c25ac599314f79175b5fe68a832f68eb867dcb2e09fe08c29e1b7d01090e",
        parent="auswahlmenue",
        ocr_text="OG",
    ),
    "warmwasser": Screen(
        hash_region=(195, 95, 290, 28),  # "Trinkwasserspeicher 1" title
        expected_hash="3e329f2a8fe5f37caa13d1f440f83af0dd8c3eb0bcc46c9f44b9a90823921279",
        parent="auswahlmenue",
        ocr_text="Trinkwasserspeicher",
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
        ocr_text="Fussbodenheizung",
    ),
    # Pellet-auger config + schedule. Intermediate screen between kundenmenue
    # and the probe-switching screen. The "Automatische Saugsondenumschalteinheit"
    # button in the center is the forward edge to that screen. back_xy overrides
    # the default (35,30) because these two screens have a dedicated title bar
    # at y=0-28 and the back-arrow button sits below at y=55-95 — (35,30) would
    # land in the dead grey area above the button.
    "saugaustragung": Screen(
        hash_region=(220, 2, 220, 28),  # "Saugaustragung" title
        expected_hash="be352e08d3bdc222cd5adec10064cf0a347e3a40fc6aa9117bd43b1bb308e26c",
        parent="kundenmenue",
        back_xy=(45, 80),
        ocr_text="Saugaustragung",
    ),
    # Pellet-storage probe overview. The 6 green/red squares at the bottom are
    # the per-zone fill indicators read by probe_dot_state(); the rest of the
    # screen has mode + threshold settings.
    "automatische_saugsondenumschalteinheit": Screen(
        hash_region=(85, 5, 470, 28),  # long "Automatische Saugsondenumschalteinheit" title
        expected_hash="e788065b21e3c24810a7774b525c0fafb6e39831140d6ac08b5399f3ddbe5f5c",
        parent="saugaustragung",
        back_xy=(45, 80),
        ocr_text="Saugsond",  # umlaut-free substring, tolerant of OCR on long title
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
    ("kundenmenue",        "saugaustragung"):     (250, 130),  # "Saugaustragung" button, top row middle
    ("saugaustragung",     "automatische_saugsondenumschalteinheit"): (395, 333),  # full-width button under "Einmalige Saugung"
}

# Per field: which screen, where to crop, how to OCR, how to parse.
# `invert=True` flips the image before OCR — helps for white-on-blue (status bars)
# and white-on-grey (status text in heat-circuit screens).
# `engine="template"` bypasses tesseract entirely and uses deterministic
# template matching against the digit glyphs in ./templates/ — needed for
# fields where tesseract's LSTM path produces different output across CPU
# instruction sets (AVX2 vs AVX512). Only use for pure-digit fields.
@dataclass
class FieldSpec:
    screen: str
    bbox: tuple[int, int, int, int]  # x, y, w, h
    config: str                      # tesseract --psm + whitelist (unused if engine="template")
    kind: str                        # "float" | "int" | "str"
    invert: bool = False
    engine: str = "tesseract"        # "tesseract" | "template"

BBOXES: dict[str, FieldSpec] = {
    # main screen
    # Bboxes on the three numeric fields were very sensitive to Tesseract's
    # 2x-upscale segmentation — a 2px shift + 5px width trim made the
    # difference between "69" and a spurious "869" read on kesseltemperatur,
    # between "21.0" and "21.0." on restsauerstoffgehalt (the trailing dot
    # poisoned parse_value), and between "9" and an empty read on
    # outside_temperature (whose tight 35x25 window left no padding for
    # Tesseract's LSTM to seed from). All three were retuned 2026-04-24.
    "kesseltemperatur":     FieldSpec("main", (258, 386,  95, 24), FIELD_NUM,  "float"),
    "restsauerstoffgehalt": FieldSpec("main", (258, 414,  95, 24), FIELD_NUM,  "float"),
    "outside_temperature":  FieldSpec("main", (550,  35,  60, 40), FIELD_NUM,  "float"),
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
    # Bbox retuned 2026-04-28: the dark-on-grey "Keine Anforderung an Kessel"
    # row sits at y=412-426; the old (100, 398, 440, 30) only grazed the top
    # half of the glyphs, producing OCR like "Keine AÄnforderuna an Kessel".
    "kessel_status_text":  FieldSpec("kessel", ( 60, 408, 540, 30), FIELD_TEXT, "str", invert=True),
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
    # bbox tightened: was (165, 178, 90, 28) which included the "°C" unit text
    # to the right of the digits — tesseract read the unit + digits as one
    # blob and hallucinated "so" / dropped digits. Narrowing to digits-only
    # (65px wide, shifted +5px right) reads cleanly and still fits "70.7".
    "ww_ist_temp":   FieldSpec("warmwasser", (170, 175,  65, 32), FIELD_NUM,  "float"),
    "ww_soll_temp":  FieldSpec("warmwasser", (460, 215, 140, 28), FIELD_NUM,  "float"),
    "ww_modus":      FieldSpec("warmwasser", (290, 365, 150, 25), FIELD_TEXT, "str"),
    # Betriebsstundenzähler page 3 — Wärmeverteilung (heat distribution counters).
    # og_h and fussbodenheizung_h use engine="template": tesseract's LSTM path
    # produces different output on AVX2 (NUC14 nodes) vs AVX512 (dev host),
    # consistently misreading these two rows. Template matching bypasses the
    # LSTM entirely and is CPU-independent.
    "rla_pumpe_h":         FieldSpec("betriebsstunden_p3", (410,  95, 140, 28), FIELD_NUM, "float"),
    "og_h":                FieldSpec("betriebsstunden_p3", (410, 135, 140, 28), FIELD_NUM, "float", engine="template"),
    "fussbodenheizung_h":  FieldSpec("betriebsstunden_p3", (410, 165, 140, 28), FIELD_NUM, "float", engine="template"),
    # Heizkreis Fussbodenheizung (live floor-heating circuit) — rows ~5px higher than OG
    # Bbox retuned twice on 2026-04-28: the original (365, 320, 80, 22)
    # was returning None on every read; (360, 322, 90, 30) read "1" instead
    # of "31" because the 90-px width truncated the leading digit on the
    # production capture. (355, 320, 110, 30) is wide enough to reliably
    # catch both digits and verifies cleanly against captures showing
    # values 31 and 33.
    "fbh_vorlauftemperatur":     FieldSpec("heizkreise_fbh", (355, 320, 110, 30), FIELD_NUM,  "float"),
    "fbh_vorlaufsolltemperatur": FieldSpec("heizkreise_fbh", (365, 350, 80, 24), FIELD_NUM,  "float"),
    "fbh_mischerposition":       FieldSpec("heizkreise_fbh", (365, 378, 80, 22), FIELD_NUM,  "float"),
    "fbh_status_text":           FieldSpec("heizkreise_fbh", (130, 410, 360, 28), FIELD_TEXT, "str", invert=True),
    "fbh_heizkreis_status":      FieldSpec("heizkreise_fbh", ( 80, 445, 520, 22), FIELD_TEXT, "str", invert=True),
    # Saugaustragung screen — AUTO/MAN mode is handled out of band in
    # saugaustragung_mode() via color sampling of the green selection frame
    # (both circle labels are always on-screen, so OCR alone can't distinguish).
    # Bbox retuned 2026-04-29: production was reading "US," — the leading
    # "A" was getting truncated by PSM 8 (single-word mode) on a tight bbox.
    # Wider window (420, 280, 65, 26) + PSM 7 (line mode) gives Tesseract
    # enough horizontal context to find the baseline and reads "Aus" cleanly.
    "einmalige_saugung":         FieldSpec("saugaustragung", (420, 280, 65, 26), FIELD_TEXT, "str", invert=True),
    # Automatische Saugsondenumschalteinheit screen — probe dots are handled
    # out of band in probe_dot_state() via PROBE_DOT_REGIONS.
    "sondenumschaltung_mode":    FieldSpec("automatische_saugsondenumschalteinheit", (415, 115, 155, 28), FIELD_TEXT, "str", invert=True),
    "info_leerer_sonden":        FieldSpec("automatische_saugsondenumschalteinheit", (330, 168,  55, 34), FIELD_NUM,  "int", invert=True),
}

# Fill-level bar interior (white when empty, dark when pellets present).
FILL_BAR_REGION: Optional[tuple[int, int, int, int]] = (545, 175, 15, 150)
FILL_BAR_FILLED_THRESHOLD = 200  # grayscale below this = filled pixel

# 8x8 center crop of each of the 6 probe indicator squares on the
# automatische_saugsondenumschalteinheit screen. Green=has pellets, red=empty.
# Centers were found by column-scanning the bottom strip of a known-good
# capture for saturated green/red pixels.
PROBE_DOT_REGIONS: dict[int, tuple[int, int, int, int]] = {
    1: (38, 436, 8, 8),
    2: (91, 436, 8, 8),
    3: (147, 436, 8, 8),
    4: (202, 436, 8, 8),
    5: (256, 436, 8, 8),
    6: (312, 436, 8, 8),
}

# Pixels on the bright-green selection frame around each Saugaustragung
# section. When AUTO is active the frame wraps the top (AUTO) section; when
# MAN is active it wraps the bottom (MAN) section. We sample the left edge
# of each potential frame — whichever is saturated green is the active mode.
SAUGAUSTRAGUNG_AUTO_FRAME_XY = (15, 200)
SAUGAUSTRAGUNG_MAN_FRAME_XY  = (15, 420)

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
    # Lower bound 10 (not -10) keeps OCR misreads where Tesseract drops
    # the leading digit of a 2-digit value ("44" → "4") from sneaking past
    # the delta_override breaker. Buffer tanks don't run below ~10°C in
    # any reasonable operating state — anything lower is OCR error.
    "puffer_temp_top":                ( 10, 120),
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
    "info_leerer_sonden":             (0, 6),
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
    "puffer_temp_bottom":          40.0,   # °C — widened 2026-04-30: full boiler-ignition cycle pushes
                                            # bottom-of-tank from ~30 to ~70 in a single 5-min interval,
                                            # so the prior 20 °C cap was rejecting legitimate ramps.
                                            # Sanity-bounds (-10, 120) still catch impossible values.
    "restsauerstoffgehalt":         5.0,   # %
    # NOTE: fill_level_percent intentionally NOT delta-checked. This field
    # is the burner's internal pellet hopper, refilled in bursts by the
    # Saugaustragung suction system pulling from the main storage silo —
    # so it legitimately cycles 0 → 100% in minutes when a suction kicks
    # in. The static SANITY_BOUNDS (0, 100) are sufficient sanity; any
    # delta cap would chronically reject the normal volatility.
    "og_vorlauftemperatur":         8.0,   # °C
    "og_vorlaufsolltemperatur":    20.0,   # °C — setpoint can jump on schedule
    "og_mischerposition":          30.0,   # %  — mixer can slam open/shut
    "fbh_vorlauftemperatur":       15.0,   # widened 2026-04-30: FBH ramp during heating exceeds 8 °C
                                            # cap regularly (real swings ~10-12 °C/cycle); 15 leaves
                                            # margin without admitting impossible reads.
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

# Deadlock-breaker for legitimate large changes. If a field reads the same (≈)
# out-of-delta value across N consecutive cycles, accept it — OCR misreads
# don't repeat pixel-identically, so persistent agreement implies the physical
# value genuinely moved. At SCRAPE_INTERVAL_SECONDS=300, N=3 = ~15 min of
# confirmation before overriding. Tracker is in-memory (resets on pod restart,
# which is fine: the retained MQTT value is then refreshed on first cycle).
_DELTA_CONFIRM: dict[str, tuple[float, int]] = {}
DELTA_CONFIRM_THRESHOLD = 3

# Same idea but for the counter monotonicity guard. Without this breaker, a
# single OCR-inflated read that gets retained as the new baseline pins the
# counter forever — every subsequent (correct, lower) read trips "decreased
# from", with no path back. With this, three consecutive same-ish lower reads
# override the inflated baseline. Tolerance is generous because hour counters
# tick by ≤0.1/cycle so consecutive correct reads agree to within ~1.
_DECREASE_CONFIRM: dict[str, tuple[float, int]] = {}
DECREASE_CONFIRM_TOLERANCE = 1.0

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
    # "Brenner Füllstand" (was "Pellets Füllstand") — this is the burner's
    # internal hopper that the Saugaustragung suction system tops up; not the
    # silo level. The new name disambiguates it from the storage tank.
    "fill_level_percent":   SensorMeta("Brenner Füllstand", "%", None, "measurement"),
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
    # Saugaustragung / probe-switching configuration (probe dots are
    # binary_sensors, registered separately in publish_discovery)
    "saugaustragung_mode":           SensorMeta("Saugaustragung Modus"),
    "einmalige_saugung":             SensorMeta("Einmalige Saugung"),
    "sondenumschaltung_mode":        SensorMeta("Sondenumschaltung"),
    "info_leerer_sonden":            SensorMeta("Info-Schwelle leerer Sonden"),
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

# Screen-identification resilience metrics. Before these, an unknown-screen
# incident manifested as a generic "navigation_failed" run status with no hint
# of why — hash-match failure vs. unknown sub-screen vs. genuinely-offline heater
# all looked identical. Splitting them lets alerts target the actual cause.
m_screen_ident = Counter(
    "solarfocus_scraper_screen_identified_total",
    "Screens identified by the recognizer, split by detection method",
    ["screen", "via"],  # via: "hash" | "ocr"
)
m_screen_unknown = Counter(
    "solarfocus_scraper_screen_unknown_total",
    "Captures where no known screen matched (neither hash nor OCR)",
)
m_nav_escape = Counter(
    "solarfocus_scraper_nav_escape_total",
    "navigate_to triggered escape-hatch after 3 consecutive unknown screens",
)
m_nav_abort = Counter(
    "solarfocus_scraper_nav_abort_total",
    "navigate_to aborted after 5 consecutive unknown screens",
)

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
    big = c.resize((c.width * 2, c.height * 2), Image.LANCZOS)
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

def probe_dot_state(img: Image.Image, region: tuple[int, int, int, int]) -> Optional[bool]:
    """True if dot is predominantly green (probe has pellets), False if red
    (probe empty), None if neither channel clearly dominates — most likely a
    miscalibrated bbox pointing at grey chrome. The 1.2x margin rejects
    anti-aliased edges and grey borders without rejecting legitimate saturated
    dots."""
    rgb = crop(img, region).convert("RGB")
    px = list(rgb.getdata())
    r_sum = sum(p[0] for p in px)
    g_sum = sum(p[1] for p in px)
    if g_sum > r_sum * 1.2:
        return True
    if r_sum > g_sum * 1.2:
        return False
    return None

def saugaustragung_mode(img: Image.Image) -> Optional[str]:
    """Detect whether AUTO or MAN is the currently-selected mode on the
    Saugaustragung screen. Both labelled circles are always visible, so OCR
    can't tell which is active — but the UI draws a bright-green rectangle
    around the selected section, so a single pixel sample on each frame's
    left edge is enough to classify."""
    rgb = img.convert("RGB")
    def is_green(xy: tuple[int, int]) -> bool:
        r, g, b = rgb.getpixel(xy)
        return g > 150 and g > r + 60 and g > b + 60
    if is_green(SAUGAUSTRAGUNG_AUTO_FRAME_XY):
        return "AUTO"
    if is_green(SAUGAUSTRAGUNG_MAN_FRAME_XY):
        return "MAN"
    return None

# =============================================================================
# Template-matching OCR (deterministic, CPU-independent)
# =============================================================================
#
# Tesseract's LSTM engine produces different output on different CPU SIMD paths
# (AVX2 on the k8s nodes, AVX512 on the dev host), which was consistently
# misreading og_h and fussbodenheizung_h on the Betriebsstundenzähler p3 screen.
# Templates sidestep the whole LSTM by comparing binarized digit glyphs pixel-
# wise against per-digit PNG templates captured once from a known-good render.
# Same pixels → same glyphs → same output, regardless of CPU.

TEMPLATES_DIR = Path(__file__).parent / "templates"
TEMPLATE_BINARIZE_THRESHOLD = 128   # grey < this → dark (digit ink)
TEMPLATE_COL_MIN_DARK = 1           # ≥1 dark pixel → column is part of a glyph

_TEMPLATE_CACHE: Optional[dict[str, Image.Image]] = None

def _load_templates() -> dict[str, Image.Image]:
    """Lazy-load digit templates from ./templates/<d>.png as binarized L-mode."""
    global _TEMPLATE_CACHE
    if _TEMPLATE_CACHE is not None:
        return _TEMPLATE_CACHE
    loaded: dict[str, Image.Image] = {}
    if TEMPLATES_DIR.exists():
        for p in sorted(TEMPLATES_DIR.glob("*.png")):
            digit = p.stem
            if digit in "0123456789":
                loaded[digit] = _binarize(Image.open(p).convert("L"))
    _TEMPLATE_CACHE = loaded
    return loaded

def _binarize(gray: Image.Image) -> Image.Image:
    """Threshold a grayscale PIL image to pure black/white."""
    return gray.point(lambda p: 0 if p < TEMPLATE_BINARIZE_THRESHOLD else 255, mode="L")

def _segment_digits(crop_img: Image.Image) -> list[Image.Image]:
    """Split a horizontal strip into per-digit sub-crops by column-scan whitespace
    detection. Returns list of binarized glyph crops, each tight-cropped in x and y.
    Empty list if no dark pixels found."""
    gray = crop_img.convert("L")
    binarized = _binarize(gray)
    w, h = binarized.size
    px = binarized.load()

    # column-scan: True where column has ≥TEMPLATE_COL_MIN_DARK dark pixels
    col_has_ink = [
        sum(1 for y in range(h) if px[x, y] == 0) >= TEMPLATE_COL_MIN_DARK
        for x in range(w)
    ]
    # find runs of ink columns → each run is one glyph
    runs: list[tuple[int, int]] = []
    start = None
    for x, ink in enumerate(col_has_ink):
        if ink and start is None:
            start = x
        elif not ink and start is not None:
            runs.append((start, x))
            start = None
    if start is not None:
        runs.append((start, w))

    glyphs: list[Image.Image] = []
    for x0, x1 in runs:
        # vertical tight-crop within this run
        col_slice = binarized.crop((x0, 0, x1, h))
        spx = col_slice.load()
        cw, ch = col_slice.size
        top, bot = 0, ch
        for y in range(ch):
            if any(spx[x, y] == 0 for x in range(cw)):
                top = y
                break
        for y in range(ch - 1, -1, -1):
            if any(spx[x, y] == 0 for x in range(cw)):
                bot = y + 1
                break
        if bot > top:
            glyphs.append(col_slice.crop((0, top, cw, bot)))
    return glyphs

def _match_glyph(glyph: Image.Image, templates: dict[str, Image.Image]) -> Optional[str]:
    """Return the digit whose template is closest to `glyph` (by sum of
    pixelwise absolute difference after resizing glyph to template size).
    None if no templates loaded."""
    if not templates:
        return None
    from PIL import ImageChops
    best_digit: Optional[str] = None
    best_score = float("inf")
    for digit, tpl in templates.items():
        resized = glyph.resize(tpl.size, Image.LANCZOS)
        resized = _binarize(resized)
        diff = ImageChops.difference(resized, tpl)
        score = sum(diff.getdata())
        if score < best_score:
            best_score = score
            best_digit = digit
    return best_digit

def ocr_digits_template(img: Image.Image, region: tuple[int, int, int, int]) -> str:
    """Deterministic digit OCR via template matching. Returns empty string
    on failure (no templates loaded, no glyphs found, or all glyphs below
    confidence threshold)."""
    templates = _load_templates()
    if not templates:
        return ""
    strip = crop(img, region)
    glyphs = _segment_digits(strip)
    if not glyphs:
        return ""
    digits: list[str] = []
    for g in glyphs:
        d = _match_glyph(g, templates)
        if d is None:
            # Partial result is worse than no result — let sanity_check fail
            # the cycle so we notice rather than publish garbage.
            return ""
        digits.append(d)
    return "".join(digits)

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

    # Last-run timestamp. The scraper publishes a fresh tz-aware ISO8601 string
    # to scraper/last_run after every successful cycle. HA's MQTT integration
    # silently drops state-equal updates (no last_changed bump), which is why
    # binding a "last run" tile to scraper_status.last_changed shows a stale
    # time across long stretches of `ok`. This separate timestamp entity
    # always changes, so HA always records the new value.
    broker.publish(f"{MQTT_DISCOVERY_PREFIX}/sensor/{MQTT_DEVICE_ID}/scraper_last_run/config", {
        "name": "Scraper Last Run",
        "unique_id": f"{MQTT_DEVICE_ID}_scraper_last_run",
        "object_id": f"{MQTT_DEVICE_ID}_scraper_last_run",
        "state_topic": f"{MQTT_TOPIC_PREFIX}/scraper/last_run",
        "device_class": "timestamp",
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

    # Per-probe fill indicators. on=green (has pellets), off=red (empty).
    # device_class is deliberately omitted — these aren't "problems" in the HA
    # sense (any single empty probe is normal during a suck cycle), and the
    # count of empty probes crossing info_leerer_sonden is the alertable signal.
    for i in PROBE_DOT_REGIONS:
        field = f"pellet_probe_{i}_full"
        broker.publish(f"{MQTT_DISCOVERY_PREFIX}/binary_sensor/{MQTT_DEVICE_ID}/{field}/config", {
            "name": f"Pellet Probe {i}",
            "unique_id": f"{MQTT_DEVICE_ID}_{field}",
            "object_id": f"{MQTT_DEVICE_ID}_{field}",
            "state_topic": f"{MQTT_TOPIC_PREFIX}/{field}",
            "payload_on": "on",
            "payload_off": "off",
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
        self.last_status: str = "(none)"         # ok|busy|navigation_failed|sanity_failed|paused|maintenance|error
        self.last_error: Optional[str] = None
        # Maintenance mode: in-process kill switch toggled from the status page's
        # Stop/Start buttons. Deliberately not MQTT-backed so it works even when
        # the broker is unreachable. Not persisted across pod restarts — a fresh
        # pod comes back in normal running mode.
        self.maintenance_mode: bool = False
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
            self.cycle_phase = "done" if status in ("ok", "paused", "maintenance", "partial") else "error"
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

    def set_current_screen(self, screen: Optional[str]) -> None:
        """Lightweight update of the current-screen pointer without capturing
        a new PNG. Used after the post-cycle return-to-main navigation so the
        status page reflects the heater's actual parked state, not just the
        last OCR'd page."""
        with self.state_lock:
            self.current_screen = screen

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

    def set_maintenance(self, on: bool) -> None:
        with self.state_lock:
            self.maintenance_mode = on

    def is_maintenance(self) -> bool:
        with self.state_lock:
            return self.maintenance_mode

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
                "maintenance_mode": self.maintenance_mode,
                # Per-screen last-capture timestamp for the status page's
                # Screens table — drops the PNG bytes, keeps only the ts.
                "per_screen_ts": {name: ts for name, (_png, ts)
                                  in self.per_screen_captures.items()},
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
    """Return the static HTML skeleton. Live values come from /events (SSE)
    and are patched into named elements by the embedded JS — so the returned
    HTML does not depend on `snap` at all. The parameter is kept for the
    historical call signature."""
    del snap  # unused; live data arrives via SSE

    # Static portions: screens + edges tables are compile-time data, not
    # runtime state, so we can bake them in server-side once.
    screen_rows_parts: list[str] = []
    for name, s in SCREENS.items():
        cal = ('<span class="text-emerald-700">yes</span>' if s.expected_hash
               else '<span class="text-amber-700">TODO</span>')
        bbox_count = sum(1 for spec in BBOXES.values() if spec.screen == name)
        screen_rows_parts.append(
            f'<tr data-screen="{html_escape(name)}" class="border-b border-slate-100 transition">'
            f'<td class="py-1 pr-2 font-mono">{html_escape(name)}</td>'
            f'<td class="py-1 pr-2 text-slate-600">{html_escape(s.parent or "—")}</td>'
            f'<td class="py-1 pr-2">{cal}</td>'
            f'<td class="py-1 pr-2 text-slate-600">{bbox_count}</td>'
            f'<td class="py-1 text-slate-500 tabular-nums" data-last-scraped>—</td></tr>'
        )
    screens_tbody = "".join(screen_rows_parts)

    edge_rows_parts = "".join(
        f'<li>{html_escape(src)} <span class="text-slate-400">→</span> {html_escape(dst)} '
        f'<span class="text-slate-400">@</span> ({xy[0]}, {xy[1]})</li>'
        for (src, dst), xy in EDGES.items()
    )

    # The template below is a plain string (not an f-string) so Tailwind
    # arbitrary-value syntax like `max-h-[500px]` and JS object literals
    # don't need brace-doubling. Substitution happens via str.replace on
    # well-named sentinels.
    return _STATUS_HTML_TEMPLATE \
        .replace("__SCREENS_COUNT__", str(len(SCREENS))) \
        .replace("__EDGES_COUNT__", str(len(EDGES))) \
        .replace("__SCREENS_TBODY__", screens_tbody) \
        .replace("__EDGES_LIST__", edge_rows_parts)


_STATUS_HTML_TEMPLATE = """<!doctype html>
<html lang="en" class="h-full">
<head>
<meta charset="utf-8">
<title>Solarfocus Scraper</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="data:,">
<script src="/static/tailwind.js"></script>
</head>
<body class="bg-slate-50 text-slate-900 min-h-full antialiased">
<div class="max-w-7xl mx-auto p-6 space-y-4">

<!-- Header: title + action buttons -->
<header class="flex flex-wrap items-center justify-between gap-3">
  <div>
    <h1 class="text-2xl font-semibold tracking-tight">Solarfocus Scraper</h1>
    <p class="text-sm text-slate-500">Live status · pushes via server-sent events</p>
  </div>
  <div class="flex items-center gap-2">
    <span id="running-badge" class="hidden inline-flex items-center gap-1.5 rounded-md bg-sky-100 text-sky-800 px-2.5 py-1 text-xs font-medium ring-1 ring-sky-200">
      <span class="inline-block w-2 h-2 rounded-full bg-sky-500 animate-pulse"></span>
      <span id="running-phase">running</span>
    </span>
    <span id="maintenance-badge" class="hidden inline-flex items-center gap-1.5 rounded-md bg-amber-100 text-amber-900 px-2.5 py-1 text-xs font-semibold ring-1 ring-amber-300">
      ⏸ maintenance
    </span>
    <button id="btn-start" type="button"
      class="inline-flex items-center gap-1.5 rounded-md bg-emerald-600 hover:bg-emerald-500 text-white px-3 py-1.5 text-sm font-medium shadow-sm transition disabled:opacity-50 disabled:cursor-not-allowed">
      <svg class="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor"><path d="M6 4l10 6-10 6V4z"/></svg>
      Start
    </button>
    <button id="btn-stop" type="button"
      class="inline-flex items-center gap-1.5 rounded-md bg-rose-600 hover:bg-rose-500 text-white px-3 py-1.5 text-sm font-medium shadow-sm transition disabled:opacity-50 disabled:cursor-not-allowed">
      <svg class="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor"><rect x="5" y="4" width="10" height="12" rx="1"/></svg>
      Stop
    </button>
  </div>
</header>

<!-- Status strip -->
<div class="bg-white rounded-lg shadow-sm ring-1 ring-slate-200 p-4">
  <div class="flex flex-wrap items-center gap-x-4 gap-y-1 text-sm">
    <span class="text-slate-500">Last cycle</span>
    <span id="status-badge" class="inline-flex items-center rounded-md px-2 py-0.5 text-xs font-medium bg-slate-100 text-slate-600">—</span>
    <span class="text-slate-400">·</span>
    <span class="text-slate-500">finished <span id="cycle-iso" class="text-slate-700 tabular-nums">—</span> (<span id="cycle-age" class="text-slate-700 tabular-nums">—</span> ago)</span>
    <span class="text-slate-400">·</span>
    <span class="text-slate-500">duration <span id="cycle-duration" class="text-slate-700 tabular-nums">—</span></span>
    <span class="text-slate-400">·</span>
    <span class="text-slate-500">current <strong id="current-screen" class="text-slate-900 font-mono">—</strong><span id="target-screen" class="text-slate-600 font-mono"></span></span>
  </div>
  <div id="error-banner" class="hidden mt-3 rounded-md bg-rose-50 border border-rose-200 text-rose-800 px-3 py-2 text-sm">
    <strong>⚠ last error:</strong> <span id="error-text" class="font-mono"></span>
  </div>
</div>

<!-- Grid: screenshot + values -->
<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
  <div class="bg-white rounded-lg shadow-sm ring-1 ring-slate-200 p-4">
    <div class="flex items-center justify-between mb-3">
      <h2 class="font-semibold text-sm text-slate-700">Last screenshot</h2>
      <span class="text-xs text-slate-500"><span id="screenshot-screen" class="font-mono">—</span> · <span id="screenshot-age" class="tabular-nums">—</span> ago</span>
    </div>
    <img id="screenshot" class="w-full rounded-md ring-1 ring-slate-200" src="/screenshot.png" alt="heater screen">
  </div>
  <div class="bg-white rounded-lg shadow-sm ring-1 ring-slate-200 p-4">
    <div class="flex items-center justify-between mb-3">
      <h2 class="font-semibold text-sm text-slate-700">Values <span id="values-count" class="text-slate-400 font-normal">(0)</span></h2>
      <input id="values-filter" type="search" placeholder="filter…"
        class="text-xs rounded-md border border-slate-300 px-2 py-1 focus:outline-none focus:ring-2 focus:ring-sky-500 focus:border-sky-500">
    </div>
    <div class="max-h-[560px] overflow-y-auto">
      <table class="w-full text-xs">
        <thead class="sticky top-0 bg-white border-b border-slate-200">
          <tr>
            <th class="text-left py-1 pr-2 font-semibold text-slate-600">field</th>
            <th class="text-left py-1 pr-2 font-semibold text-slate-600">value</th>
            <th class="text-left py-1 font-semibold text-slate-600">age</th>
          </tr>
        </thead>
        <tbody id="values-tbody"></tbody>
      </table>
    </div>
  </div>
</div>

<!-- Screens (collapsible) -->
<details class="bg-white rounded-lg shadow-sm ring-1 ring-slate-200 p-4">
  <summary class="font-semibold text-sm text-slate-700 cursor-pointer select-none">Screens <span class="text-slate-400 font-normal">(__SCREENS_COUNT__)</span></summary>
  <table class="w-full text-xs mt-3">
    <thead>
      <tr class="border-b border-slate-200">
        <th class="text-left py-1 pr-2 font-semibold text-slate-600">name</th>
        <th class="text-left py-1 pr-2 font-semibold text-slate-600">parent</th>
        <th class="text-left py-1 pr-2 font-semibold text-slate-600">calibrated</th>
        <th class="text-left py-1 pr-2 font-semibold text-slate-600">bboxes</th>
        <th class="text-left py-1 font-semibold text-slate-600">last scraped</th>
      </tr>
    </thead>
    <tbody id="screens-tbody">__SCREENS_TBODY__</tbody>
  </table>
</details>

<!-- Edges (collapsible) -->
<details class="bg-white rounded-lg shadow-sm ring-1 ring-slate-200 p-4">
  <summary class="font-semibold text-sm text-slate-700 cursor-pointer select-none">Edges <span class="text-slate-400 font-normal">(__EDGES_COUNT__)</span></summary>
  <ul class="mt-3 font-mono text-xs text-slate-700 space-y-0.5 pl-4 list-disc">
    __EDGES_LIST__
  </ul>
</details>

<footer class="text-xs text-slate-400 pt-2 pb-8 text-center">
  <span id="sse-status">connecting…</span>
</footer>

</div>

<script>
(() => {
  const $ = id => document.getElementById(id);
  let lastScreenshotTs = 0;
  let lastSnap = null;

  const STATUS_STYLES = {
    ok:                'bg-emerald-100 text-emerald-800 ring-1 ring-emerald-200',
    partial:           'bg-sky-100 text-sky-800 ring-1 ring-sky-200',
    paused:            'bg-slate-200 text-slate-700 ring-1 ring-slate-300',
    maintenance:       'bg-amber-100 text-amber-900 ring-1 ring-amber-300',
    busy:              'bg-amber-100 text-amber-900 ring-1 ring-amber-300',
    navigation_failed: 'bg-rose-100 text-rose-800 ring-1 ring-rose-200',
    sanity_failed:     'bg-rose-100 text-rose-800 ring-1 ring-rose-200',
    error:             'bg-rose-100 text-rose-800 ring-1 ring-rose-200',
  };

  const fmtAge = s => {
    if (s == null || !isFinite(s) || s < 0) return '—';
    if (s < 60)   return Math.round(s) + 's';
    if (s < 3600) return Math.floor(s/60) + 'm' + Math.round(s%60).toString().padStart(2,'0') + 's';
    return (s/3600).toFixed(1) + 'h';
  };

  const fmtVal = v => {
    if (v === null || v === undefined) return '—';
    if (v === true)  return '●';
    if (v === false) return '○';
    return String(v);
  };

  const valClass = v => {
    if (v === true)  return 'font-mono font-semibold text-emerald-600 text-base leading-none';
    if (v === false) return 'font-mono font-semibold text-rose-600 text-base leading-none';
    return 'font-mono font-semibold text-slate-900';
  };

  function update(snap) {
    lastSnap = snap;

    // Running badge
    const rb = $('running-badge');
    if (snap.cycle_running) {
      rb.classList.remove('hidden');
      $('running-phase').textContent =
        snap.cycle_phase + (snap.cycle_phase_detail ? ' ' + snap.cycle_phase_detail : '');
    } else {
      rb.classList.add('hidden');
    }

    // Maintenance badge + button states
    const inMaint = !!snap.maintenance_mode;
    $('maintenance-badge').classList.toggle('hidden', !inMaint);
    $('btn-start').disabled = !inMaint;
    $('btn-stop').disabled = inMaint;

    // Status badge
    const sb = $('status-badge');
    const s = snap.last_status || '—';
    sb.textContent = s;
    sb.className = 'inline-flex items-center rounded-md px-2 py-0.5 text-xs font-medium ' +
                   (STATUS_STYLES[s] || 'bg-slate-100 text-slate-600');

    // Cycle stats — render in the browser's local TZ. toISOString() returns UTC,
    // which is misleading for a user who reads the timestamp at a glance and
    // compares it with HA / the wall clock. Build the local-time string by hand.
    if (snap.cycle_completed_ts) {
      const d = new Date(snap.cycle_completed_ts * 1000);
      const p = (n) => String(n).padStart(2, '0');
      $('cycle-iso').textContent =
        `${d.getFullYear()}-${p(d.getMonth()+1)}-${p(d.getDate())} ` +
        `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
    }
    $('cycle-age').textContent = snap.cycle_completed_ts
      ? fmtAge((Date.now() / 1000) - snap.cycle_completed_ts) : '—';
    $('cycle-duration').textContent = snap.last_duration_s
      ? snap.last_duration_s.toFixed(1) + 's' : '—';
    $('current-screen').textContent = snap.current_screen || '—';
    $('target-screen').textContent = snap.target_screen ? ' → ' + snap.target_screen : '';

    // Error banner
    const eb = $('error-banner');
    if (snap.last_error) {
      eb.classList.remove('hidden');
      $('error-text').textContent = snap.last_error;
    } else {
      eb.classList.add('hidden');
    }

    // Screenshot — only reload src when ts changed, so the image doesn't flicker
    if (snap.last_screenshot_ts && snap.last_screenshot_ts !== lastScreenshotTs) {
      lastScreenshotTs = snap.last_screenshot_ts;
      $('screenshot').src = '/screenshot.png?t=' + lastScreenshotTs;
      $('screenshot-screen').textContent = snap.last_screenshot_screen || '—';
    }
    if (snap.last_screenshot_ts) {
      $('screenshot-age').textContent =
        fmtAge((Date.now() / 1000) - snap.last_screenshot_ts);
    }

    // Highlight current screen row + stamp per-screen last-scrape age
    const nowS = Date.now() / 1000;
    const perTs = snap.per_screen_ts || {};
    document.querySelectorAll('tr[data-screen]').forEach(tr => {
      const name = tr.dataset.screen;
      const isCurrent = name === snap.current_screen;
      tr.classList.toggle('bg-sky-50', isCurrent);
      tr.classList.toggle('font-semibold', isCurrent);
      const cell = tr.querySelector('[data-last-scraped]');
      if (cell) {
        const ts = perTs[name];
        cell.textContent = ts ? fmtAge(nowS - ts) + ' ago' : '—';
        cell.title = ts ? new Date(ts * 1000).toISOString().replace('T', ' ').slice(0, 19) + ' UTC' : '';
      }
    });

    // Values table: diff-patch in place so scroll position is kept
    const keys = Object.keys(snap.values || {}).sort();
    $('values-count').textContent = '(' + keys.length + ')';
    const tbody = $('values-tbody');
    const existing = new Map(Array.from(tbody.children).map(tr => [tr.dataset.field, tr]));
    for (const [field, tr] of existing) {
      if (!snap.values[field]) tr.remove();
    }
    let prev = null;
    for (const field of keys) {
      const rec = snap.values[field];
      let tr = existing.get(field);
      if (!tr) {
        tr = document.createElement('tr');
        tr.dataset.field = field;
        tr.className = 'border-b border-slate-100';
        tr.innerHTML =
          '<td class="py-1 pr-2 font-mono text-slate-700"></td>' +
          '<td class="py-1 pr-2"></td>' +
          '<td class="py-1 text-slate-500 tabular-nums"></td>';
        if (prev) prev.after(tr); else tbody.prepend(tr);
      }
      const [td0, td1, td2] = tr.children;
      td0.textContent = field;
      td1.innerHTML = '<span class="' + valClass(rec.value) + '">' + fmtVal(rec.value) + '</span>' +
        (rec.unit ? ' <span class="text-slate-400">' + rec.unit + '</span>' : '');
      td2.textContent = fmtAge(rec.age_s);
      prev = tr;
    }
    applyFilter();
  }

  function applyFilter() {
    const q = $('values-filter').value.trim().toLowerCase();
    document.querySelectorAll('#values-tbody tr').forEach(tr => {
      const match = !q || tr.dataset.field.toLowerCase().includes(q);
      tr.classList.toggle('hidden', !match);
    });
  }

  $('values-filter').addEventListener('input', applyFilter);

  async function action(path) {
    const btn = path === '/api/start' ? $('btn-start') : $('btn-stop');
    btn.disabled = true;
    try {
      const r = await fetch(path, { method: 'POST' });
      if (!r.ok) throw new Error('HTTP ' + r.status);
    } catch (e) {
      console.error('action failed', path, e);
      btn.disabled = false;
    }
    // SSE update will re-compute button state — no need to re-enable here.
  }
  $('btn-start').addEventListener('click', () => action('/api/start'));
  $('btn-stop').addEventListener('click',  () => action('/api/stop'));

  function connectSSE() {
    const src = new EventSource('/events');
    src.onopen = () => { $('sse-status').textContent = 'connected · live updates every 2s'; };
    src.onmessage = e => {
      try { update(JSON.parse(e.data)); }
      catch (err) { console.error('SSE parse error', err); }
    };
    src.onerror = () => { $('sse-status').textContent = 'reconnecting…'; };
  }

  // Tick the age-display fields every second without needing a new SSE message.
  setInterval(() => {
    if (!lastSnap) return;
    const nowS = Date.now() / 1000;
    if (lastSnap.cycle_completed_ts) {
      $('cycle-age').textContent = fmtAge(nowS - lastSnap.cycle_completed_ts);
    }
    if (lastSnap.last_screenshot_ts) {
      $('screenshot-age').textContent = fmtAge(nowS - lastSnap.last_screenshot_ts);
    }
    const perTs = lastSnap.per_screen_ts || {};
    document.querySelectorAll('tr[data-screen]').forEach(tr => {
      const cell = tr.querySelector('[data-last-scraped]');
      if (!cell) return;
      const ts = perTs[tr.dataset.screen];
      if (ts) cell.textContent = fmtAge(nowS - ts) + ' ago';
    });
  }, 1000);

  connectSSE();
})();
</script>
</body>
</html>"""


STATIC_DIR = Path(__file__).parent / "static"
SSE_INTERVAL_SECONDS = 2.0  # how often /events pushes a snapshot to connected clients
# Each /events connection runs in its own ThreadingHTTPServer thread, and any
# per-connection state (twisted/jsonlogger artifacts, thread stacks, leaked
# refs) only frees on handler return. Capping connection lifetime bounds the
# memory we can leak per session — the browser's EventSource auto-reconnects
# in ~3s when we close, so the cycle is invisible to the user.
SSE_MAX_CONNECTION_SECONDS = 300.0


def _snapshot_json(snap: dict) -> str:
    """Serialize a Coordinator snapshot for SSE. ValueRecord instances are
    flattened to plain dicts so the client JS doesn't need class shapes.
    bool is serialized as JSON true/false so the UI can badge probe dots."""
    values_out: dict[str, dict] = {}
    for field, rec in snap["values"].items():
        values_out[field] = {
            "value": rec.value,
            "ts_iso": rec.iso(),
            "age_s": round(rec.age_s(), 1),
            "unit": (SENSORS.get(field).unit if SENSORS.get(field) else None),
        }
    out = {k: v for k, v in snap.items() if k != "values"}
    out["values"] = values_out
    return json.dumps(out, default=str)


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
            # Healthz must not report unhealthy when the user has explicitly
            # stopped the scraper — maintenance is an intended idle state,
            # not a failure, so k8s probes should stay green.
            ok = age < stale_after or COORD.is_maintenance()
            self._send(200 if ok else 503, "text/plain", f"age={age:.0f}s\n".encode())
        elif path in ("/", "/status"):
            html = render_status_html(COORD.snapshot())
            self._send(200, "text/html; charset=utf-8", html.encode("utf-8"))
        elif path == "/events":
            # Server-Sent Events stream. One persistent connection per client
            # (ThreadingHTTPServer handles concurrency); yields a snapshot
            # every SSE_INTERVAL_SECONDS. Client-side EventSource auto-reconnects
            # on TCP drop, so we don't need retry logic on our side. We also
            # close the connection after SSE_MAX_CONNECTION_SECONDS to bound
            # per-connection memory growth from any leaked thread state.
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")  # disable nginx proxy buffering
            self.end_headers()
            deadline = time.monotonic() + SSE_MAX_CONNECTION_SECONDS
            try:
                while time.monotonic() < deadline:
                    payload = _snapshot_json(COORD.snapshot())
                    self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                    self.wfile.flush()
                    time.sleep(SSE_INTERVAL_SECONDS)
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass  # client disconnected; just end the handler
            return  # graceful timeout — browser EventSource will reconnect
        elif path.startswith("/static/"):
            # Serve vendored assets (tailwind.js etc). Strict path check keeps
            # the server from reading outside the static/ directory.
            rel = path[len("/static/"):]
            if not rel or "/" in rel or rel.startswith(".") or ".." in rel:
                self._send(400, "text/plain", b"bad path\n")
                return
            fpath = STATIC_DIR / rel
            if not fpath.is_file():
                self._send(404, "text/plain", b"not found\n")
                return
            ctype = "application/javascript" if rel.endswith(".js") else \
                    "text/css" if rel.endswith(".css") else \
                    "application/octet-stream"
            self._send(200, ctype, fpath.read_bytes())
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

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path == "/api/stop":
            COORD.set_maintenance(True)
            event(logging.WARNING, "maintenance_on",
                  "maintenance mode enabled via /api/stop")
            self._send(200, "application/json",
                       b'{"ok":true,"maintenance":true}')
        elif path == "/api/start":
            COORD.set_maintenance(False)
            event(logging.INFO, "maintenance_off",
                  "maintenance mode disabled via /api/start")
            self._send(200, "application/json",
                       b'{"ok":true,"maintenance":false}')
        else:
            self._send(404, "text/plain", b"not found\n")

def start_http_server() -> None:
    # ThreadingHTTPServer is required now that /events holds a long-lived
    # SSE stream per client — a single-threaded HTTPServer would block
    # /metrics and /healthz behind any connected browser.
    srv = ThreadingHTTPServer(("0.0.0.0", METRICS_PORT), _HealthHandler)
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


class _MaintenanceAbort(Exception):
    """Raised mid-cycle when the user hits Stop on the status page. The outer
    try/finally in run_cycle disconnects VNC immediately, freeing the single
    VNC slot for the user's manual session — the whole point of the Stop
    button is "I need the heater touchscreen now, don't make me wait 30s for
    this cycle to finish."""

def _identify_screen(img: Image.Image) -> Optional[str]:
    """Return the name of the matching known screen, or None.

    Two-stage match:
      1. Fast path — exact SHA256 of `hash_region`. Zero-cost but brittle; any
         VNC compression jitter or firmware-driven pixel shift invalidates it.
      2. Fallback — OCR `ocr_region` (defaults to `hash_region`) and look for
         `ocr_text` as a case-insensitive substring. Used to be the source of
         the "unknown screen, tapping back" incident: hash had drifted on a
         perfectly-normal screen and we kept blindly tapping back forever.

    When the OCR path matches but the hash didn't, log the drifted hash at
    WARNING so the operator can refresh `expected_hash` in source (self-heal).
    """
    # Fast path — exact hash
    for name, screen in SCREENS.items():
        if not screen.expected_hash:
            continue
        if region_hash(img, screen.hash_region) == screen.expected_hash:
            m_screen_ident.labels(screen=name, via="hash").inc()
            return name
    # Fallback — OCR title/distinctive text for screens that opted in.
    for name, screen in SCREENS.items():
        if not screen.ocr_text:
            continue
        region = screen.ocr_region or screen.hash_region
        try:
            text = ocr(img, region, FIELD_TEXT, lang="deu")
        except Exception as e:
            event(logging.DEBUG, "identify_ocr_failed", "OCR attempt failed",
                  screen=name, error=str(e))
            continue
        if screen.ocr_text.lower() in text.lower():
            m_screen_ident.labels(screen=name, via="ocr").inc()
            if screen.expected_hash:
                new_hash = region_hash(img, screen.hash_region)
                if new_hash != screen.expected_hash:
                    event(logging.WARNING, "screen_hash_drift",
                          "OCR matched but hash differs — update expected_hash in main.py",
                          screen=name,
                          new_hash=new_hash,
                          old_hash=screen.expected_hash,
                          ocr_text_seen=text.strip()[:120])
            return name
    m_screen_unknown.inc()
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

    Unknown-screen recovery (identify returned None):
      - streak < 3: simple tap of BACK_ARROW_XY (covers most in-flow
        transitional states where a redraw is in progress).
      - streak == 3: escape-hatch — tap (5,5) + BACK_ARROW_XY + 2s wait.
        A few heater screens render the back arrow outside the default
        bbox, or have overlay widgets covering it; the corner-tap clears
        most of those.
      - streak >= 5: abort the cycle cleanly rather than flailing
        forever. Before this, navigate_to could eat a cycle indefinitely
        tapping back on a screen its templates had no word for.
    """
    if target not in SCREENS:
        event(logging.ERROR, "navigate_unknown_target", "no such screen", target=target)
        return False
    unknown_streak = 0
    for step in range(max_steps):
        img = vnc_capture(client)
        current = _identify_screen(img)
        if current == target:
            event(logging.DEBUG, "navigate_reached", "at target", target=target, steps=step)
            return True
        if current is None:
            unknown_streak += 1
            if unknown_streak >= 5:
                m_nav_abort.inc()
                event(logging.ERROR, "navigate_unknown_abort",
                      "5 consecutive unknowns — aborting before we drift further",
                      step=step, target=target)
                return False
            if unknown_streak == 3:
                m_nav_escape.inc()
                event(logging.WARNING, "navigate_escape",
                      "3 consecutive unknowns — trying escape hatch (corner + back)",
                      step=step, target=target)
                vnc_click(client, 5, 5)
                time.sleep(CLICK_DELAY_SECONDS)
                vnc_click(client, *BACK_ARROW_XY)
                time.sleep(2.0)
                continue
            event(logging.INFO, "navigate_unknown_screen",
                  "unknown screen, tapping back",
                  step=step, target=target, streak=unknown_streak)
            vnc_click(client, *BACK_ARROW_XY)
            time.sleep(CLICK_DELAY_SECONDS)
            continue
        # Identified a known screen — reset the unknown streak.
        unknown_streak = 0
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
        if spec.engine == "template":
            raw = ocr_digits_template(img, spec.bbox)
        else:
            raw = ocr(img, spec.bbox, spec.config, invert=spec.invert)
        parsed = parse_value(raw, spec.kind)
        out[field] = parsed
        event(logging.DEBUG, "ocr_result", "ocr value",
              field=field, engine=spec.engine, raw=raw, parsed=parsed)
    if FILL_BAR_REGION is not None:
        main_img = img_by_screen.get("main")
        if main_img is not None:
            out["fill_level_percent"] = fill_level_percent(main_img)
    saug_img = img_by_screen.get("saugaustragung")
    if saug_img is not None:
        out["saugaustragung_mode"] = saugaustragung_mode(saug_img)
    probe_img = img_by_screen.get("automatische_saugsondenumschalteinheit")
    if probe_img is not None:
        for i, region in PROBE_DOT_REGIONS.items():
            out[f"pellet_probe_{i}_full"] = probe_dot_state(probe_img, region)
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


def _sanity_check(values: dict[str, object], broker: Optional[MqttBroker],
                  allow_delta_override: bool = False) -> dict[str, str]:
    """Return a dict of {field: reject_reason} — empty if all values passed.

    Three layers per field:
      1. Static bounds (SANITY_BOUNDS) — physical range.
      2. Counter monotonicity (COUNTER_FIELDS) — hour meters never decrease.
      3. Delta check (MAX_DELTA_PER_CYCLE) — plausible change between cycles.
         Skipped when no prior value is known.

    `allow_delta_override=True` enables the deadlock-breaker on TWO paths:
      - Delta-exceed: persistent out-of-delta reads get counted in
        _DELTA_CONFIRM and accepted after DELTA_CONFIRM_THRESHOLD matching
        cycles.
      - Counter-decreased: persistent below-baseline reads get counted in
        _DECREASE_CONFIRM and accepted on the same threshold. This recovers
        from OCR-inflated retained baselines that would otherwise pin the
        counter forever (the monotonicity guard refuses any lower value).
    Only set True on the retry pass of run_cycle so we count once per cycle,
    not once per sanity call.

    Every field is checked in one pass; run_cycle publishes the accepted
    fields and holds back only the rejected ones.
    """
    rejected: dict[str, str] = {}
    for field, val in values.items():
        if val is None:
            continue
        # Skip numeric checks for bool values (isinstance(True, int) is True
        # in Python, so without this guard a probe's True/False would collide
        # with any future SANITY_BOUNDS/COUNTER_FIELDS/MAX_DELTA entry on the
        # same field name).
        if isinstance(val, bool):
            continue
        bounds = SANITY_BOUNDS.get(field)
        if bounds and isinstance(val, (int, float)):
            lo, hi = bounds
            if not (lo <= val <= hi):
                rejected[field] = f"{val} out of bounds [{lo}, {hi}]"
                continue
        if field in COUNTER_FIELDS and broker and isinstance(val, (int, float)):
            prev_str = broker.get_last(field)
            if prev_str:
                try:
                    prev = float(prev_str)
                    if val < prev:
                        # Same N-confirmation breaker as MAX_DELTA_PER_CYCLE.
                        # Catches OCR-inflated retained baselines that have
                        # locked the counter at an unreachably-high value:
                        # if the field reads consistently lower for N cycles,
                        # the inflated baseline is wrong and the new value
                        # should be accepted as truth.
                        if allow_delta_override:
                            tracked = _DECREASE_CONFIRM.get(field)
                            if tracked and abs(val - tracked[0]) <= DECREASE_CONFIRM_TOLERANCE:
                                count = tracked[1] + 1
                            else:
                                count = 1
                            if count >= DELTA_CONFIRM_THRESHOLD:
                                event(logging.WARNING, "decrease_override_accepted",
                                      "accepting decreased counter after persistent confirmation",
                                      field=field, value=val, prev=prev,
                                      confirmations=count)
                                _DECREASE_CONFIRM.pop(field, None)
                                continue
                            _DECREASE_CONFIRM[field] = (val, count)
                            rejected[field] = (
                                f"{val} decreased from {prev} "
                                f"(confirm {count}/{DELTA_CONFIRM_THRESHOLD})")
                        else:
                            rejected[field] = f"{val} decreased from {prev}"
                        continue
                    elif allow_delta_override:
                        # No longer decreasing — clear the tracker.
                        _DECREASE_CONFIRM.pop(field, None)
                except ValueError:
                    pass
        max_delta = MAX_DELTA_PER_CYCLE.get(field)
        if max_delta is not None and broker and isinstance(val, (int, float)):
            prev_str = broker.get_last(field)
            if prev_str:
                try:
                    prev = float(prev_str)
                    over = abs(val - prev) > max_delta
                    if over and allow_delta_override:
                        tol = max(1.0, max_delta * 0.1)
                        tracked = _DELTA_CONFIRM.get(field)
                        if tracked and abs(val - tracked[0]) <= tol:
                            count = tracked[1] + 1
                        else:
                            count = 1
                        if count >= DELTA_CONFIRM_THRESHOLD:
                            event(logging.WARNING, "delta_override_accepted",
                                  "accepting out-of-delta value after persistent confirmation",
                                  field=field, value=val, prev=prev,
                                  delta=val - prev, max_delta=max_delta,
                                  confirmations=count)
                            _DELTA_CONFIRM.pop(field, None)
                            continue
                        _DELTA_CONFIRM[field] = (val, count)
                        rejected[field] = (
                            f"{val} delta={val - prev:+.1f} exceeds ±{max_delta} "
                            f"from prev={prev} (confirm {count}/{DELTA_CONFIRM_THRESHOLD})")
                    elif over:
                        rejected[field] = (
                            f"{val} delta={val - prev:+.1f} exceeds ±{max_delta} "
                            f"from prev={prev}")
                    elif allow_delta_override:
                        _DELTA_CONFIRM.pop(field, None)
                except ValueError:
                    pass
    return rejected

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
        if COORD.is_maintenance():
            event(logging.INFO, "maintenance", "scraper in maintenance mode (Stop button), skipping cycle")
            m_runs.labels(status="maintenance").inc()
            if broker and not dry_run:
                broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/status", "maintenance", retain=True)
            final_status = "maintenance"
            return CycleResult(status="maintenance", values={})
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
                    # Check the Stop button between every screen — the user's
                    # manual VNC session is blocked as long as this cycle holds
                    # the single VNC slot, so bail out at the next safe seam
                    # rather than finishing all 8 screens.
                    if COORD.is_maintenance():
                        raise _MaintenanceAbort()
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
            except _MaintenanceAbort:
                event(logging.INFO, "maintenance_abort",
                      "cycle aborted mid-flight by Stop button, releasing VNC")
                m_runs.labels(status="maintenance").inc()
                if broker and not dry_run:
                    broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/status", "maintenance", retain=True)
                final_status = "maintenance"
                return CycleResult(status="maintenance", values={})

            # First sanity pass. If any field trips, re-capture + re-OCR
            # everything once — the heater's UI occasionally redraws mid-frame
            # and a slightly different capture resolves transient OCR misreads
            # (e.g. '35' read as '357' when the bbox overlaps a row boundary).
            # On the retry pass we enable the delta-override so persistent
            # out-of-delta reads can advance their confirm counter.
            rejected = _sanity_check(values, broker)
            if rejected:
                event(logging.WARNING, "sanity_retry",
                      "sanity failed on first pass, re-capturing + re-OCR'ing",
                      rejected=rejected)
                try:
                    values_retry = _capture_and_ocr()
                except _NavFail as nf:
                    final_status = "navigation_failed"
                    final_error = f"could not reach {nf.screen} (on retry)"
                    return _handle_nav_fail(client, broker, dry_run, nf.screen)
                except _MaintenanceAbort:
                    event(logging.INFO, "maintenance_abort",
                          "cycle aborted mid-retry by Stop button, releasing VNC")
                    m_runs.labels(status="maintenance").inc()
                    if broker and not dry_run:
                        broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/status", "maintenance", retain=True)
                    final_status = "maintenance"
                    return CycleResult(status="maintenance", values={})
                values = values_retry
                rejected = _sanity_check(values, broker, allow_delta_override=True)
        finally:
            # Best-effort: park the heater on the main screen so (a) the next
            # cycle always starts from a known state, and (b) when the user
            # walks up to the physical touchscreen they see the overview
            # rather than whatever sub-page we happened to OCR last. Skip
            # when maintenance was just triggered — the whole point of Stop
            # is to release VNC *now*, not click a few more times first.
            if not COORD.is_maintenance():
                try:
                    if navigate_to(client, "main", max_steps=6):
                        COORD.set_current_screen("main")
                except Exception as e:
                    event(logging.DEBUG, "return_to_main_failed",
                          "best-effort return to main failed",
                          error=str(e))
            try:
                client.disconnect()
            except Exception:
                pass

        # Partial publish: skip rejected fields, publish the rest. One stuck
        # OCR field must not freeze the other 33. `sanity_failed` is reserved
        # for the edge case where every readable field was rejected — that
        # implies the scrape is globally broken (wrong screen, alert modal).
        accepted = {f: v for f, v in values.items() if v is not None and f not in rejected}
        if rejected and not accepted:
            event(logging.ERROR, "sanity_check_failed",
                  "all fields rejected", rejected=rejected)
            m_runs.labels(status="sanity_failed").inc()
            if broker and not dry_run:
                broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/status", "sanity_failed", retain=True)
            final_status, final_error = "sanity_failed", "all fields rejected"
            return CycleResult(status="sanity_failed", values=values)

        if rejected:
            event(logging.WARNING, "sanity_partial",
                  "cycle publishing with some fields held back",
                  rejected=rejected, accepted_count=len(accepted))

        if broker and not dry_run:
            COORD.set_phase("publishing")
            if first_run_ref and first_run_ref[0]:
                publish_discovery(broker)
                first_run_ref[0] = False
            for field, val in accepted.items():
                # bool MUST come before str() — isinstance(True, int) is True in
                # Python, and "True"/"False" is not what HA's binary_sensor
                # discovery expects; it'd leave the entity as "unavailable"
                # until someone publishes "on"/"off".
                if isinstance(val, bool):
                    payload = "on" if val else "off"
                else:
                    payload = str(val)
                # Retain sensor values so HA recovers state on restart and the
                # delta check in _sanity_check has a stable baseline after a
                # pod restart. Non-retained publishes were dropping HA entities
                # to "unknown" after every HA reload.
                broker.publish(f"{MQTT_TOPIC_PREFIX}/{field}", payload, retain=True)
                event(logging.DEBUG, "mqtt_published", "value published", field=field, value=val)
            status = "partial" if rejected else "ok"
            broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/status", status, retain=True)
            broker.publish(f"{MQTT_TOPIC_PREFIX}/scraper/last_run",
                           datetime.now(timezone.utc).isoformat(), retain=True)

        duration = time.time() - COORD.cycle_started_ts
        m_last_run.set(time.time())
        m_last_dur.set(duration)
        result_status = "partial" if rejected else "ok"
        m_runs.labels(status=result_status).inc()
        final_status = result_status
        event(logging.INFO, "cycle_complete",
              f"cycle {result_status}", duration_s=round(duration, 2),
              accepted_count=len(accepted), rejected_count=len(rejected))
        return CycleResult(status=result_status, values=values)
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

def cmd_learn_templates(args) -> None:
    """Extract per-digit PNG templates from a known-good capture.

    Usage:
      python main.py learn-templates IMAGE  x,y,w,h  EXPECTED_DIGITS

    Example:
      python main.py learn-templates screenshots/p3.png 410,135,140,28 36177

    The bbox is segmented by column whitespace; each glyph is saved as
    templates/<digit>.png matching the corresponding character in
    EXPECTED_DIGITS. Running multiple times overwrites earlier templates —
    run against your cleanest capture.
    """
    img = Image.open(args.image)
    region = args.region
    strip = crop(img, region)
    glyphs = _segment_digits(strip)
    expected = args.expected
    if len(glyphs) != len(expected):
        sys.exit(f"segmentation mismatch: got {len(glyphs)} glyphs, "
                 f"expected {len(expected)} for {expected!r}. "
                 f"Check the bbox or the expected string.")
    TEMPLATES_DIR.mkdir(exist_ok=True)
    for g, d in zip(glyphs, expected):
        if d not in "0123456789":
            print(f"skipping non-digit {d!r}")
            continue
        dest = TEMPLATES_DIR / f"{d}.png"
        g.save(dest)
        print(f"wrote {dest} ({g.size[0]}x{g.size[1]})")
    # Invalidate cache so next OCR picks up fresh templates.
    global _TEMPLATE_CACHE
    _TEMPLATE_CACHE = None
    # Round-trip self-check.
    tpl = _load_templates()
    readback = "".join(_match_glyph(g, tpl) or "?" for g in glyphs)
    status = "ok" if readback == expected else "MISMATCH"
    print(f"self-check: read back {readback!r} (expected {expected!r}) — {status}")

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

    plt = sub.add_parser("learn-templates",
        help="extract per-digit PNG templates from a known-good capture")
    plt.add_argument("image", help="path to a saved screenshot")
    plt.add_argument("region", type=_parse_region,
        help="x,y,w,h of a horizontal digit strip")
    plt.add_argument("expected",
        help="the digit string the strip should contain (e.g. 36177)")
    plt.set_defaults(func=cmd_learn_templates)

    pr = sub.add_parser("run", help="production loop")
    pr.add_argument("--no-mqtt", action="store_true", help="dev mode: skip MQTT, just status UI + cycles")
    pr.add_argument("--interval", type=int, default=0, help="override SCRAPE_INTERVAL_SECONDS")
    pr.set_defaults(func=cmd_run)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
