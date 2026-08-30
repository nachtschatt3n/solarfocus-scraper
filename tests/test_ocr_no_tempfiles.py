#!/usr/bin/env python3
"""Regression guard for the OCR temp-file churn that looked like a memory leak.

Background (2026-08-30). The pod's `container_memory_working_set_bytes` climbed
~10 MiB/day for 12 days straight, to 231 MiB of a 256 MiB limit, with 0
restarts. It was not a Python leak: the container cgroup read `anon` 96.9 MiB
(flat — in fact trending DOWN 2.7 MiB/day), `file` 11.5 MiB, `sock` 0,
`process_open_fds` 12 and flat — but `slab_reclaimable` 132.8 MiB and climbing.
cAdvisor's working set is `memory.current - inactive_file`, which *includes*
reclaimable slab, so kernel dentry/inode cache was being read as application
memory.

That slab came from `pytesseract.image_to_string`, which creates and unlinks
THREE temp files per call (`tess_XXXX`, `tess_XXXX_input.png`, and tesseract's
`tess_XXXX.txt`). At ~60-70 OCR calls per cycle and ~185 cycles/day that is
~36k file create+unlink pairs a day. `main._tesseract()` pipes PNG bytes to
tesseract's stdin and reads stdout instead, touching the filesystem zero times.

Two properties are pinned here:

1. **No temp files.** A full sweep of `ocr()` calls must not construct a single
   temp file. Checked by instrumenting `tempfile`, not by diffing a directory —
   pytesseract cleans up after itself, so a before/after listing would pass
   while the dentry churn (the actual cause) continued unabated.
2. **Identical output.** The stdin/stdout path must return exactly what
   pytesseract's temp-file path returned, for every real `FieldSpec` in
   `BBOXES` that a committed fixture covers, plus both alert-modal regions.
   That is what makes the swap safe in a codebase where OCR values are
   load-bearing and otherwise uncovered by unit tests.

Run either way:
    python tests/test_ocr_no_tempfiles.py     (no pytest needed; exits non-zero)
    pytest tests/
Needs tesseract with the `deu` language pack, same as the scraper.
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from PIL import Image  # noqa: E402

import main  # noqa: E402

FIXTURES = REPO / "tests" / "fixtures"

# Which committed fixture shows which screen, so we can drive the REAL FieldSpecs
# rather than invented bboxes.
FIXTURE_SCREENS = {
    "screen_probe.png": "main",
    "screen_saugaustragung.png": "saugaustragung",
    "warmwasser_74.png": "warmwasser",
    "warmwasser_75.png": "warmwasser",
}


def _cases() -> list[tuple[str, tuple[int, int, int, int], str, dict]]:
    """(fixture, bbox, config, ocr-kwargs) for every covered field + both modals."""
    out = []
    for fixture, screen in FIXTURE_SCREENS.items():
        for spec in main.BBOXES.values():
            if spec.screen != screen or spec.engine != "tesseract":
                continue
            out.append((fixture, spec.bbox, spec.config,
                        {"invert": spec.invert, "lcd": spec.lcd}))
    out.append(("alert_modal_wartung.png", main.ALERT_TITLE_BBOX,
                main.FIELD_TEXT, {"invert": True}))
    out.append(("alert_modal_wartung.png", main.ALERT_BODY_BBOX,
                main.FIELD_PARAGRAPH, {}))
    out.append(("alert_modal_pelletsmangel_inset.png", main.ALERT_BODY_BBOX,
                main.FIELD_PARAGRAPH, {}))
    return out


CASES = _cases()


def _pytesseract_reference(img, region, config, lang="deu", invert=False, lcd=False):
    """`main.ocr()` verbatim, but through pytesseract — the pre-fix behaviour."""
    import pytesseract
    from PIL import ImageOps

    c = main.crop(img, region)
    if invert:
        c = ImageOps.invert(c.convert("L"))
    if lcd:
        gray = c.convert("L")
        big = gray.resize((gray.width * 3, gray.height * 3), Image.LANCZOS)
        t = main._otsu_threshold(big)
        big = big.point(lambda p: 0 if p < t else 255, mode="L")
    else:
        big = c.resize((c.width * 2, c.height * 2), Image.LANCZOS)
    return pytesseract.image_to_string(big, lang=lang, config=config).strip()


def test_ocr_creates_no_temp_files():
    """The leak regression. Reverting ocr() to pytesseract fails this."""
    assert CASES, "no OCR cases derived — fixture/screen mapping is stale"
    calls: list[str] = []
    originals = {
        name: getattr(tempfile, name)
        for name in ("NamedTemporaryFile", "TemporaryFile", "mkstemp", "mkdtemp")
    }

    def _record(name, fn):
        def wrapper(*a, **kw):
            calls.append(name)
            return fn(*a, **kw)
        return wrapper

    for name, fn in originals.items():
        setattr(tempfile, name, _record(name, fn))
    try:
        for fixture, bbox, config, kwargs in CASES:
            img = Image.open(FIXTURES / fixture)
            main.ocr(img, bbox, config, **kwargs)
    finally:
        for name, fn in originals.items():
            setattr(tempfile, name, fn)

    assert not calls, (
        f"ocr() made {len(calls)} tempfile call(s) across {len(CASES)} OCR calls "
        f"({sorted(set(calls))}). Every temp file created here becomes "
        "dentry+inode slab charged to the container's memory cgroup and counted "
        "in container_memory_working_set_bytes — that is the 'memory leak' this "
        "test exists to prevent regressing.")


def test_stdin_path_matches_pytesseract_exactly():
    """Correctness gate: swapping the transport must not change a single glyph."""
    try:
        import pytesseract  # noqa: F401
    except ImportError:
        print("SKIP: pytesseract not installed, cannot diff against the old path")
        return

    mismatches = []
    for fixture, bbox, config, kwargs in CASES:
        img = Image.open(FIXTURES / fixture)
        got = main.ocr(img, bbox, config, **kwargs)
        want = _pytesseract_reference(img, bbox, config, **kwargs)
        if got != want:
            mismatches.append(f"{fixture} {bbox} {config!r} {kwargs}: "
                              f"stdin={got!r} pytesseract={want!r}")
    assert not mismatches, (
        f"OCR output changed on {len(mismatches)}/{len(CASES)} cases:\n  "
        + "\n  ".join(mismatches))


def test_missing_tesseract_raises():
    """A broken tesseract invocation must raise, not silently return ''.

    Silence here would be worse than a crash: an empty OCR read parses to None,
    which _sanity_check skips rather than rejects, so every sensor would quietly
    stop updating while the cycle still logged 'ok'.
    """
    img = Image.new("L", (40, 20), 255)
    old = main.TESSERACT_CMD
    main.TESSERACT_CMD = "definitely-not-a-real-tesseract-binary"
    try:
        try:
            main._tesseract(img, lang="deu", config=main.FIELD_NUM)
        except RuntimeError as e:
            assert "not found" in str(e), f"unexpected error text: {e}"
        else:
            raise AssertionError("missing tesseract did not raise")
    finally:
        main.TESSERACT_CMD = old


TESTS = [
    test_ocr_creates_no_temp_files,
    test_stdin_path_matches_pytesseract_exactly,
    test_missing_tesseract_raises,
]


def _main() -> int:
    print(f"{len(CASES)} OCR cases derived from BBOXES + alert modals\n")
    failed = 0
    for t in TESTS:
        try:
            t()
        except AssertionError as e:
            print(f"FAIL  {t.__name__}\n      {e}")
            failed += 1
        except Exception as e:  # noqa: BLE001
            print(f"ERROR {t.__name__}: {type(e).__name__}: {e}")
            failed += 1
        else:
            print(f"PASS  {t.__name__}")
    print(f"\n{len(TESTS) - failed}/{len(TESTS)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_main())
