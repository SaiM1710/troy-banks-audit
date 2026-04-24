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

        # Skip empty lines
        if not line:
            continue

        # Skip per-unit rate lines like "0.079489 x 304 kWh"
        if re.search(r'\d+\.\d{5,}\s*x', line):
            continue

        # Skip usage history table rows (month/kWh pairs)
        if re.match(r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+', line):
            continue

        # Skip lines that are just numbers (graph artifacts from OCR)
        if re.match(r'^[\d\s]+$', line) and len(line) < 20:
            continue

        # Skip website URLs
        if 'www.' in line.lower() or 'http' in line.lower():
            continue

        # Skip phone number lines
        if re.match(r'^1-\d{3}-\d{3}-\d{4}', line):
            continue

        # Skip lines with percentage rates only (no dollar amount)
        if re.match(r'^\d+\.\d+\s*%\s*$', line):
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
            'surcharge', 'adjustment', 'credit', 'please pay by'
        ]
        if any(kw in line.lower() for kw in important_keywords):
            cleaned_lines.append(line)
            continue

    return '\n'.join(cleaned_lines)


def call_ollama(prompt: str, schema: dict = None) -> str:
    """Single reusable Ollama call. Schema is optional."""
    url = "http://localhost:11434/api/generate"

    payload = {
        "model": "qwen2.5-coder:7b",
        "prompt": prompt,
        "stream": True,
        "options": {
            "temperature": 0.1,
            "num_ctx": 2048,
            "num_predict": 1024
        }
    }

    if schema:
        payload["format"] = schema

    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        url, data=data, headers={'Content-Type': 'application/json'}
    )

    try:
        full_response = ""
        with urllib.request.urlopen(req) as response:
            for chunk_bytes in response:
                for packet in chunk_bytes.decode('utf-8').split('\n'):
                    if packet.strip():
                        chunk = json.loads(packet)
                        token = chunk.get('response', '')
                        print(token, end='', flush=True)
                        full_response += token

        print()
        return full_response.replace('```json', '').replace('```', '').strip()

    except urllib.error.URLError as e:
        return f'{{"error": "Ollama connection failed: {str(e)}"}}'
    except Exception as e:
        return f'{{"error": "{str(e)}"}}'


def stage1_extract_line_items(cleaned_text: str) -> str:
    """
    Stage 1: Extract every labeled dollar amount from cleaned bill text.
    Forces JSON array output via schema enforcement.
    """
    print("\n[STAGE 1] Extracting line items from bill...")

    array_schema = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "label":  {"type": "string"},
                "amount": {"type": "number"}
            },
            "required": ["label", "amount"]
        }
    }

    prompt = f"""
Extract every line item that has a dollar amount from this utility bill.
Return a JSON array. Each item has "label" and "amount".
Credits and reductions are negative numbers.
Include section totals like "Total Delivery Services".
Include the final "Amount Due".
Do not include per-unit rates.

Bill Text:
{cleaned_text}
"""
    return call_ollama(prompt, schema=array_schema)


def stage2_map_to_schema(line_items_json: str, cleaned_text: str) -> str:
    """
    Stage 2: Map extracted line items to our unified schema.
    Uses strict schema enforcement.
    """
    print("\n[STAGE 2] Mapping line items to unified schema...")

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

    prompt = f"""
You are a utility bill data mapper.
Map the extracted line items to the correct schema fields.

EXTRACTED LINE ITEMS:
{line_items_json}

BILL TEXT (for dates, account number, usage):
{cleaned_text}

MAPPING RULES:

provider_name: The utility company name at the top of the bill.

account_number: The number next to "ACCOUNT NUMBER". Digits only, no dashes.

utility_type: "Electric" if kWh. "Gas" if Therms or CCF.

statement_date: Date next to "DATE BILL ISSUED". Format YYYY-MM-DD.

service_period_start: First date in the billing period. Format YYYY-MM-DD.

service_period_end: Last date in the billing period. Format YYYY-MM-DD.

usage_volume: Total energy used as a number. Example: 304 from "304 kWh".

usage_unit: "kWh" for Electric. "Therms" for Gas — even if bill says CCF.

rate_code: Text after "RATE" label. Example: "Electric SC1 Non Heat".

fixed_monthly_charge: 0.0 if Basic Service is already inside Total Delivery Services.
Only populate if Basic Service is billed completely separately from delivery.

delivery_charge: Use "Total Delivery Services" amount exactly.
If no section total, sum all delivery related items.

supply_charge: Use "Total Supply Services" amount exactly.
If no section total, sum all supply related items.

demand_charge: Peak kW demand charge. Use 0.0 if not present.

taxes_and_surcharges: Use "Total Other Charges/Adjustments" exactly.
Includes late fees not already in delivery or supply.

credits: Balance forward or payment credits that reduce the final amount.
Always a POSITIVE number. Example: "Balance Forward -1.50" means credits = 1.50.

total_amount_due: The final "Amount Due" after all credits.

is_anomaly_detected: false — Python will set this.
anomaly_reason: "" — Python will set this.

IMPORTANT:
- Use 0.0 for missing numbers.
- Use "" for missing strings.
- Never output null.
- Do not perform any math.
"""

    return call_ollama(prompt, schema=strict_schema)


def normalize_parsed_data(parsed_data: dict) -> dict:
    """
    Post-processing normalization after AI extraction.
    Handles unit standardization and data cleaning.
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

    # Clean account number — digits only
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


def parse_bill_to_json(raw_text: str, source_file: str = None) -> str:
    """
    Two-stage extraction pipeline.
    Stage 1: Extract all line items as-is from cleaned bill text.
    Stage 2: Map extracted items to our unified schema.
    """
    print(f"\n[PARSER] Starting two-stage extraction...")

    # Pre-process OCR text to remove noise
    cleaned_text = preprocess_ocr_text(raw_text)

    print(f"\n[PREPROCESSED TEXT — {len(cleaned_text.splitlines())} lines]")
    print("-" * 40)
    print(cleaned_text)
    print("-" * 40)

    # Stage 1 — extract line items from cleaned text
    stage1_output = stage1_extract_line_items(cleaned_text)

    try:
        items = json.loads(stage1_output)
        if not isinstance(items, list) or len(items) == 0:
            print("[STAGE 1 FAILED] No line items extracted.")
            return '{"error": "Stage 1 extraction produced no line items."}'
        print(f"\n[STAGE 1 COMPLETE] Extracted {len(items)} line items:")
        for item in items:
            print(f"  {item.get('label', '?')}: {item.get('amount', '?')}")
    except json.JSONDecodeError:
        print(f"[STAGE 1 FAILED] Could not parse output as JSON.")
        print(f"Raw output: {stage1_output[:300]}")
        return '{"error": "Stage 1 JSON parse failed."}'

    # Stage 2 — map to schema using cleaned text for context
    stage2_output = stage2_map_to_schema(stage1_output, cleaned_text)
    return stage2_output


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

    # Layer 2: Two-stage AI parsing
    clean_json = parse_bill_to_json(raw_text, source_file=file_path)

    try:
        parsed_data = json.loads(clean_json)
    except json.JSONDecodeError:
        print(f"[PIPELINE] Failed to parse AI output as JSON.")
        print(f"Raw AI output: {clean_json}")
        return False

    if 'error' in parsed_data:
        print(f"[PIPELINE] AI extraction error: {parsed_data['error']}")
        return False

    # Layer 3: Normalize and clean
    parsed_data = normalize_parsed_data(parsed_data)

    # Layer 4: Python math audit — always overrides AI judgment
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