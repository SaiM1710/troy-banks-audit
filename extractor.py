import fitz  # PyMuPDF
import pytesseract
from pdf2image import convert_from_path
import cv2
import numpy as np
import os
from PIL import Image  # Required to hand the cleaned image back to Tesseract
import re


def is_billing_page(text: str) -> bool:
    """
    Returns True if the page contains actual billing data.
    Checks for dollar amounts, account number patterns, or charge figures.
    Disclaimer/information pages will fail all these checks.
    """
    # Look for dollar amounts like $73.54 or 73.54
    has_dollar_amounts = bool(re.search(r'\$\s*\d+\.\d{2}', text))
    
    # Look for standalone decimal numbers that look like charges (e.g. 45.94, 27.60)
    has_charge_figures = bool(re.search(r'\b\d+\.\d{2}\b', text))
    
    # Look for account number patterns (sequences of 5+ digits)
    has_account_pattern = bool(re.search(r'\b\d{5,}\b', text))
    
    return has_dollar_amounts or (has_charge_figures and has_account_pattern)

def extract_via_ocr(file_path: str) -> str:
    """Fallback method: Uses Computer Vision and Tesseract to read scanned images."""
    print("--> Scanned document detected. Routing to Tesseract OCR...")
    try:
        pages = convert_from_path(file_path, dpi=300)
        full_text = ""
        pages_included = 0

        for i, page in enumerate(pages):
            print(f"    Cleaning and reading Page {i + 1}...")

            open_cv_image = np.array(page)
            img = open_cv_image[:, :, ::-1].copy()
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            thresh_val, thresh_img = cv2.threshold(
                gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
            )
            final_image = Image.fromarray(thresh_img)
            text = pytesseract.image_to_string(final_image, config='--psm 6')

            if is_billing_page(text):
                full_text += f"--- PAGE {i + 1} ---\n{text}\n"
                pages_included += 1
                print(f"    Page {i + 1}: billing data detected — included.")
            else:
                print(f"    Page {i + 1}: no billing data detected — skipped.")

        print(f"--> OCR complete. {pages_included} of {len(pages)} pages contained billing data.")
        return full_text

    except Exception as e:
        return f"OCR Error: {str(e)}"

def process_utility_bill(file_path: str) -> str:
    """The main entry point. Tries PyMuPDF first, falls back to OCR if needed."""
    print(f"Processing: {file_path}")
    
    try:
        # Attempt 1: PyMuPDF (Digital Extraction)
        doc = fitz.open(file_path)
        digital_text = ""
        for page in doc:
            digital_text += page.get_text("text")
            
        # Check if we actually got text. Scanned PDFs usually return < 100 chars of invisible junk.
        if len(digital_text.strip()) > 100:
            print("--> Digital PDF detected. Extraction successful via PyMuPDF.")
            return digital_text
        else:
            # Attempt 2: If PyMuPDF found nothing, it must be a scan.
            return extract_via_ocr(file_path)
            
    except Exception as e:
        return f"File Error: {str(e)}"

# --- LOCAL TEST EXECUTION ---
if __name__ == "__main__":
    # Point this to a dummy file in your main project folder
    test_file = "dummy_bill.pdf" 
    
    if os.path.exists(test_file):
        print("Starting extraction test...\n" + "="*40)
        result = process_utility_bill(test_file)
        print("\n=== RAW TEXT OUTPUT ===")
        print(result[:1500]) # Print the first 1500 characters to inspect
    else:
        print(f"Error: Could not find '{test_file}'. Please place a test PDF in the folder.")