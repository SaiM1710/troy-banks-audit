import json
import urllib.request
import urllib.error
import os
import re
import db_manager
from extractor import process_utility_bill


def preprocess_ocr_text(raw_text: str) -> str:
    """
    Cleans OCR text before sending to AI.
    Removes noise — rate tables, usage history, marketing text.
    Keeps only lines likely to contain billing data.
    """
    cleaned_lines = []

    for line in raw_text.split('\n'):
        line = line.strip()

        if not line:
            continue

        # Skip per-unit rate lines like "0.079489 x 304 kWh"
        if re.search(r'\d+\.\d{5,}\s*x', line):
            continue

        # Skip usage history table rows
        if re.match(r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+', line):
            continue

        # Skip lines that are just numbers
        if re.match(r'^[\d\s]+$', line) and len(line) < 20:
            continue

        # Skip website URLs
        if 'www.' in line.lower() or 'http' in line.lower():
            continue

        # Skip phone number lines
        if re.match(r'^1-\d{3}-\d{3}-\d{4}', line):
            continue

        # Keep lines with dollar amounts
        if re.search(r'\$\s*\d+\.?\d*|\b\d+\.\d{2}\b', line):
            cleaned_lines.append(line)
            continue

        # Keep lines with important billing keywords
        important_keywords = [
            'total', 'amount due', 'account number', 'billing period',
            'service period', 'date bill issued', 'balance forward',
            'current charges', 'rate', 'meter', 'usage', 'kwh', 'therms',
            'basic service', 'delivery', 'supply', 'demand', 'tax',
            'surcharge', 'adjustment', 'credit', 'please pay by',
            'national grid', 'conedison', 'con edison', 'keyspan',
            'rg&e', 'pg&e', 'provider', 'utility'
        ]
        if any(kw in line.lower() for kw in important_keywords):
            cleaned_lines.append(line)
            continue

    return '\n'.join(cleaned_lines)


def parse_bill_to_json(raw_text: str, source_file: str = None) -> str:
    """
    Single-stage extraction using Ollama with strict JSON schema.
    Works reliably with qwen2.5-coder:7b.
    Replace model with groq/claude when moving to production.
    """
    active_model = "qwen2.5-coder:7b"
    print(f"\n--> Transmitting to Ollama ({active_model})...")

    url = "http://localhost:11434/api/generate"

    strict_schema = {
        "type": "object",
        "properties": {
            "provider_name":        {"type": "string"},
            "account_number":       {"type": "string"},
            "utility_type":         {"type": "string", "enum": ["Electric", "Gas", "Unknown"]},
            "statement_date":       {"type": "string"},
            "service_period_start": {"type": "string"},
            "service_period_end":   {"type": "string"},
            "total_amount_due":     {"type": "number"},
            "usage_volume":         {"type": "number"},
            "usage_unit":           {"type": "string", "enum": ["kWh", "Therms"]},
            "rate_code":            {"type": "string"},
            "fixed_monthly_charge": {"type": "number"},
            "delivery_charge":      {"type": "number"},
            "supply_charge":        {"type": "number"},
            "demand_charge":        {"type": "number"},
            "taxes_and_surcharges": {"type": "number"},
            "credits":              {"type": "number"},
            "is_anomaly_detected":  {"type": "boolean"},
            "anomaly_reason":       {"type": "string"}
        },
        "required": [
            "provider_name", "account_number", "utility_type",
            "total_amount_due", "is_anomaly_detected"
        ]
    }

    # Clean the text before sending
    cleaned_text = preprocess_ocr_text(raw_text)

    prompt = f"""
You are a utility bill data extractor. Extract fields from the bill text below.
Return only valid JSON. No explanation.

RULES:
1. Dates must be YYYY-MM-DD format.
2. account_number: digits only, no dashes or spaces.
3. Use 0.0 for missing numbers. Use "" for missing strings. Never null.
4. total_amount_due: use the "Amount Due" figure — the final amount customer must pay.
5. utility_type: "Electric" if kWh, "Gas" if Therms.
6. usage_unit: "kWh" for Electric, "Therms" for Gas.
7. credits: balance forward or payment credits as POSITIVE number.

CHARGE MAPPING:
- delivery_charge: use "Total Delivery Services" figure exactly.
- supply_charge: use "Total Supply Services" figure exactly.
- fixed_monthly_charge: 0.0 if Basic Service is inside delivery total.
- taxes_and_surcharges: use "Total Other Charges/Adjustments" figure.
- demand_charge: peak kW charge only. 0.0 if not present.

Bill Text:
{cleaned_text}

Also use this for provider name and dates:
{raw_text[:300]}
"""

    payload = {
        "model": active_model,
        "prompt": prompt,
        "format": strict_schema,
        "stream": True,
        "options": {
            "temperature": 0.1,
            "num_ctx": 3072,
            "num_predict": 1024
        }
    }

    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        url, data=data, headers={'Content-Type': 'application/json'}
    )

    try:
        print(f"\n[SYSTEM] >>> GENERATING ({active_model}) <<<\n")
        full_response = ""

        with urllib.request.urlopen(req) as response:
            for chunk_bytes in response:
                for packet in chunk_bytes.decode('utf-8').split('\n'):
                    if packet.strip():
                        chunk = json.loads(packet)
                        token = chunk.get('response', '')
                        print(token, end='', flush=True)
                        full_response += token

        print("\n\n[SYSTEM] >>> GENERATION COMPLETE <<<")
        return full_response.replace('```json', '').replace('```', '').strip()

    except urllib.error.URLError as e:
        return f'{{"error": "Ollama connection failed: {str(e)}"}}'
    except Exception as e:
        return f'{{"error": "{str(e)}"}}'


def normalize_parsed_data(parsed_data: dict) -> dict:
    """
    Post-processing normalization after AI extraction.
    """
    # Standardize CCF to Therms
    if parsed_data.get('usage_unit') == 'CCF':
        parsed_data['usage_unit'] = 'Therms'
        print("[NORMALIZE] CCF standardized to Therms.")

    # Ensure utility_type matches usage_unit
    usage_unit = parsed_data.get('usage_unit', '')
    if usage_unit == 'kWh':
        parsed_data['utility_type'] = 'Electric'
    elif usage_unit == 'Therms':
        parsed_data['utility_type'] = 'Gas'

    # Clean account number
    parsed_data['account_number'] = (
        parsed_data.get('account_number', '')
        .replace('-', '').replace(' ', '').strip()
    )

    # Ensure credits is always positive
    credits = parsed_data.get('credits', 0.0)
    if credits < 0:
        parsed_data['credits'] = abs(credits)
        print(f"[NORMALIZE] Credits converted to positive: {parsed_data['credits']}")

    return parsed_data


def run_pipeline(file_path: str) -> bool:
    """
    Full pipeline: PDF -> Extract -> AI Parse -> Python Audit -> DB Insert
    """
    print(f"\n{'='*50}")
    print(f"PROCESSING: {file_path}")
    print(f"{'='*50}")

    # Layer 1: Extract text from PDF
    raw_text = process_utility_bill(file_path)
    if "Error" in raw_text:
        print(f"[PIPELINE] Halted at extraction: {raw_text}")
        return False

    # Layer 2: Single-stage AI parsing
    clean_json = parse_bill_to_json(raw_text, source_file=file_path)

    try:
        parsed_data = json.loads(clean_json)
    except json.JSONDecodeError:
        print(f"[PIPELINE] Failed to parse AI output as JSON.")
        print(f"Raw output: {clean_json}")
        return False

    if 'error' in parsed_data:
        print(f"[PIPELINE] AI error: {parsed_data['error']}")
        return False

    # Layer 3: Normalize
    parsed_data = normalize_parsed_data(parsed_data)

    # Layer 4: Python math audit
    calc_total = (
        parsed_data.get('fixed_monthly_charge', 0.0) +
        parsed_data.get('delivery_charge', 0.0) +
        parsed_data.get('supply_charge', 0.0) +
        parsed_data.get('demand_charge', 0.0) +
        parsed_data.get('taxes_and_surcharges', 0.0) -
        parsed_data.get('credits', 0.0)
    )
    actual_total = parsed_data.get('total_amount_due', 0.0)

    if abs(calc_total - actual_total) > 0.02:
        parsed_data['is_anomaly_detected'] = True
        parsed_data['anomaly_reason'] = (
            f"MATH AUDIT FAILED: Sub-charges sum to ${calc_total:.2f} "
            f"but total due is ${actual_total:.2f}. "
            f"Discrepancy of ${abs(calc_total - actual_total):.2f}."
        )
    else:
        parsed_data['is_anomaly_detected'] = False
        parsed_data['anomaly_reason'] = "Math validated by Python audit."

    print("\n=== VERIFIED JSON OUTPUT ===")
    print(json.dumps(parsed_data, indent=4))

    # Layer 5: Insert into DB
    source_filename = os.path.basename(file_path)
    return db_manager.insert_bill(parsed_data, source_file=source_filename)


if __name__ == "__main__":
    test_file = "dummy_bill.pdf"

    if os.path.exists(test_file):
        success = run_pipeline(test_file)
        if success:
            print("\n[PIPELINE] Complete. Bill stored in database.")
        else:
            print("\n[PIPELINE] Failed. Check errors above.")
    else:
        print(f"[ERROR] File not found: {test_file}")