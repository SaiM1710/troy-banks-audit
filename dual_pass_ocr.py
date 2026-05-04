"""
dual_pass_ocr.py — OCR helper that catches white-on-dark text.

The default Tesseract pass reads dark-on-light text reliably but often
drops white-on-dark text — common on bill header bands like the dark
navy "Total Electric Usage" cell where the value sits in white on dark.

This module runs two OCR passes:
  1. Normal pass on the original image (catches the bulk of the bill)
  2. Inverted pass on a flipped image (catches white-on-dark regions)

Then it merges the results so the final text contains everything either
pass found — without duplicates.

How to use it from your existing OCR pipeline:

    from dual_pass_ocr import ocr_with_dual_pass

    # Replace any existing pytesseract.image_to_string call:
    text = ocr_with_dual_pass(clean_img, config="--psm 3 --oem 3")
"""

import re
from typing import Optional

import cv2
import numpy as np
import pytesseract


# ─────────────────────────────────────────────────────────────────────────────
# Detection — is the inverted pass even worth running?
# ─────────────────────────────────────────────────────────────────────────────

def has_dark_regions(img_gray: np.ndarray,
                     darkness_threshold: int = 50,
                     min_dark_ratio: float = 0.02) -> bool:
    """
    Returns True if the image has meaningful dark regions worth inverting.
    Skips the inverted pass entirely on bills with no dark headers,
    saving ~1-2 seconds per bill.

    darkness_threshold : pixel values below this are considered "dark"
    min_dark_ratio     : at least this fraction of the image must be dark
                         to bother with the inverted pass. 2% catches header
                         bands while ignoring trivial dark lines / borders.
    """
    if img_gray is None or img_gray.size == 0:
        return False

    dark_pixels = np.sum(img_gray < darkness_threshold)
    total       = img_gray.size
    return (dark_pixels / total) >= min_dark_ratio


# ─────────────────────────────────────────────────────────────────────────────
# Merging — combine two OCR passes without duplicates
# ─────────────────────────────────────────────────────────────────────────────

def _normalise_for_dedup(line: str) -> str:
    """
    Normalises a line so identical-looking lines from both passes match
    even with minor whitespace or punctuation differences. Used only for
    duplicate detection, not for the output text itself.
    """
    return re.sub(r"\s+", " ", line.strip().lower())


def merge_passes(normal_text: str, inverted_text: str) -> str:
    """
    Combines two OCR passes into a single text string.
    Lines from the inverted pass are appended only if they don't already
    appear in the normal pass — so we don't bloat the prompt with duplicates.

    Why we keep both even when there's overlap:
      The inverted pass usually contains a lot of garbage (Tesseract
      hallucinating text from inverted areas of the image that weren't
      meant to be inverted). But it also contains the white-on-dark text
      we needed. Filtering by "not already seen" is a cheap way to keep
      the genuine additions while dropping noise that overlaps the normal
      pass.
    """
    if not normal_text and not inverted_text:
        return ""
    if not inverted_text:
        return normal_text
    if not normal_text:
        return inverted_text

    # Build a set of normalised lines we already have from the normal pass
    seen = {
        _normalise_for_dedup(line)
        for line in normal_text.splitlines()
        if line.strip()
    }

    # Find genuinely new lines from the inverted pass
    new_lines = []
    for line in inverted_text.splitlines():
        normalised = _normalise_for_dedup(line)
        if not normalised:
            continue   # skip blank lines
        if len(normalised) < 2:
            continue   # skip single characters — usually noise
        if normalised in seen:
            continue   # already in normal pass
        new_lines.append(line.rstrip())
        seen.add(normalised)

    if not new_lines:
        return normal_text

    # Append a marker so the model can tell where the extra content came from
    # (Useful for debugging — but the model treats it as just more bill text)
    return (
        normal_text.rstrip()
        + "\n\n[--- additional text from inverted pass ---]\n"
        + "\n".join(new_lines)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────

def ocr_with_dual_pass(img_gray: np.ndarray,
                        config: str = "--psm 3 --oem 3",
                        force_inverted: bool = False) -> str:
    """
    Runs Tesseract twice — normal and inverted — and merges the results.
    Recovers white-on-dark text that single-pass OCR drops.

    Parameters
    ----------
    img_gray : grayscale numpy array (output of cv2.cvtColor with BGR2GRAY)
    config   : Tesseract config string — same one you'd use normally
    force_inverted : if True, runs the inverted pass even when the image
                     has no obvious dark regions. Useful for testing.

    Returns
    -------
    Merged text string. On bills with no dark regions, this is identical
    to a single normal pass. On bills with dark headers, this contains
    both the normal text and any extra content the inverted pass found.
    """
    if img_gray is None or img_gray.size == 0:
        return ""

    # Pass 1 — normal Tesseract on the original image
    normal_text = pytesseract.image_to_string(img_gray, config=config)

    # Skip inverted pass if there's nothing dark to invert (saves time)
    if not force_inverted and not has_dark_regions(img_gray):
        return normal_text

    # Pass 2 — invert and run again
    # cv2.bitwise_not flips every pixel: white ↔ black, near-white ↔ near-black
    # White text on dark background becomes dark text on light background,
    # which Tesseract reads reliably
    inverted_img  = cv2.bitwise_not(img_gray)
    inverted_text = pytesseract.image_to_string(inverted_img, config=config)

    return merge_passes(normal_text, inverted_text)