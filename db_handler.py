"""
db_handler.py — saves extracted bill data into the Troy & Banks
relational database.

Schema this writes to (matches create_db.py exactly):
  Clients ──┬── Properties ──┬── Accounts ──┬── Bills ── Line_Items
  Vendors ─────────────────────┘                       │
                                                        └── Audit_Claims

CRITICAL CHECK constraints enforced by the database:
  - Bills.utility_type   ∈ ('Electric', 'Gas', 'Unknown')
  - Bills.usage_unit     ∈ ('kWh', 'Therms', 'Unknown')
  - Bills.anomaly_status ∈ ('Unreviewed', 'Confirmed', 'Dismissed', 'Claimed')
  - Vendors.vendor_type  ∈ ('Electric', 'Gas', 'Both', 'Other')
  - Line_Items.category  ∈ (9 values listed in normalise_category)

Any value outside these sets will be rejected with a CHECK constraint
error. This module normalises incoming values into the constraint set
before insert so the model's free-text outputs don't fail at the
database layer.

Note on usage_unit: the schema only accepts kWh / Therms / Unknown.
Water bills (CF, CCF, gallons) WILL be rejected. If you need to save
water bills, the schema needs to be expanded — this is a hard limit.
"""

import re
import sqlite3
from datetime import datetime
from pathlib import Path


# ─────────────────────────────────────────────────────────────────────────────
# Date and amount normalisation
# ─────────────────────────────────────────────────────────────────────────────

DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y",
    "%b %d, %Y", "%b %d %Y", "%m-%d-%Y"
]


def normalise_date(value: str | None) -> str | None:
    """Convert any of the known date formats to ISO YYYY-MM-DD."""
    if not value or not isinstance(value, str):
        return None
    value = value.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_amount(value) -> float:
    """
    Parse anything that looks like money into a float.
    Returns 0.0 on garbage so NOT NULL constraints don't fail.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = re.sub(r"[$,\s]", "", str(value)).replace("(", "-").replace(")", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# CHECK constraint normalisers
# ─────────────────────────────────────────────────────────────────────────────
# These functions map free-text values from the extraction layer into the
# exact set of values the database CHECK constraints will accept. Anything
# unrecognised falls back to a safe default ('Unknown' / 'Other').

def normalise_utility_type(value) -> str:
    """
    Bills.utility_type CHECK IN ('Electric', 'Gas', 'Unknown')

    Maps various inputs to the constrained set:
      - "Electric", "ELECTRIC", "electric"   → "Electric"
      - "Gas", "Natural Gas", "gas"          → "Gas"
      - water bills, unknown                 → "Unknown"

    Note: "national_grid" routes to "Electric" since most National Grid
    bills are predominantly electric. If you later differentiate by
    bill content, this mapping should split into Electric vs Gas.
    """
    if not value:
        return "Unknown"
    v = str(value).lower().strip()
    if "electric" in v or v in ("kwh", "national_grid"):
        return "Electric"
    if "gas" in v or "therm" in v:
        return "Gas"
    return "Unknown"


def normalise_usage_unit(value) -> str:
    """
    Bills.usage_unit CHECK IN ('kWh', 'Therms', 'Unknown')

    The database is stricter than the UI dropdown — water units like
    CF, CCF, gallons are NOT accepted. Such bills become 'Unknown'
    rather than failing the save outright.
    """
    if not value:
        return "Unknown"
    v = str(value).strip().lower()
    if v == "kwh":
        return "kWh"
    if v in ("therm", "therms"):
        return "Therms"
    # CF, CCF, gallons, anything else → Unknown
    return "Unknown"


def normalise_vendor_type(utility_type: str) -> str:
    """
    Vendors.vendor_type CHECK IN ('Electric', 'Gas', 'Both', 'Other')

    Derived from the bill's utility_type. We don't currently distinguish
    'Both' (dual-service vendors like National Grid) — every bill comes
    in as either Electric or Gas individually. The 'Both' value is
    reserved for future use when vendor metadata is enriched (e.g.
    detecting that the same vendor_id has bills with both utility_types).

    Allowed values: 'Electric', 'Gas', 'Both', 'Other'.
    """
    if not utility_type:
        return "Other"
    v = str(utility_type).lower()
    if "electric" in v:
        return "Electric"
    if "gas" in v:
        return "Gas"
    return "Other"


# Line_Items.category CHECK constraint — 9 allowed values
ALLOWED_CATEGORIES = {
    "Fixed Monthly Charge",
    "Delivery Charge",
    "Supply Charge",
    "Demand Charge",
    "Taxes and Surcharges",
    "Rider",
    "Adjustment",
    "Credit",
    "Other",
}


def normalise_category(value) -> str:
    """
    Line_Items.category — one of 9 allowed values, else 'Other'.

    The extraction layer (Ollama prompt) is told to return categories
    matching this set, but as a safety net any unrecognised value gets
    mapped to 'Other' so the insert doesn't fail on a CHECK constraint.

    A few common misspellings are mapped explicitly.
    """
    if not value:
        return "Other"
    v = str(value).strip()
    if v in ALLOWED_CATEGORIES:
        return v

    # Light fuzzy mapping for common variants
    v_lower = v.lower()
    mapping = {
        "fixed monthly":          "Fixed Monthly Charge",
        "customer charge":        "Fixed Monthly Charge",
        "monthly charge":         "Fixed Monthly Charge",
        "service charge":         "Fixed Monthly Charge",
        "delivery":               "Delivery Charge",
        "distribution":           "Delivery Charge",
        "transmission":           "Delivery Charge",
        "supply":                 "Supply Charge",
        "generation":             "Supply Charge",
        "energy":                 "Supply Charge",
        "demand":                 "Demand Charge",
        "tax":                    "Taxes and Surcharges",
        "surcharge":              "Taxes and Surcharges",
        "fee":                    "Taxes and Surcharges",
        "rider":                  "Rider",
        "adjustment":             "Adjustment",
        "credit":                 "Credit",
    }
    for needle, target in mapping.items():
        if needle in v_lower:
            return target
    return "Other"


# ─────────────────────────────────────────────────────────────────────────────
# Hierarchy resolvers — auto-create missing parent rows
# ─────────────────────────────────────────────────────────────────────────────

def get_or_create_client(conn, name):
    """
    Find the client by name, or create a new row if this is the first
    bill we've seen from them. Returns the client_id.
    """
    if not name:
        name = "Unknown Client"
    cur = conn.cursor()
    cur.execute("SELECT client_id FROM Clients WHERE client_name = ?", (name,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("INSERT INTO Clients (client_name) VALUES (?)", (name,))
    return cur.lastrowid


def get_or_create_property(conn, client_id, fields):
    """
    Find or create the property record. Multiple bills from the same
    physical address share one property_id, even when arriving from
    different vendors (electric + gas at the same building).
    """
    address = (
        fields.get("service_address")
        or fields.get("address")
        or "Unknown Address"
    )

    cur = conn.cursor()
    cur.execute(
        "SELECT property_id FROM Properties "
        "WHERE client_id = ? AND address = ?",
        (client_id, address)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("""
        INSERT INTO Properties (client_id, address, city, state,
                                zip_code, property_name)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        client_id,
        address,
        fields.get("city"),
        fields.get("state"),
        fields.get("zip"),
        fields.get("property_name"),
    ))
    return cur.lastrowid


def get_or_create_vendor(conn, vendor_name, utility_type):
    """
    Find or create the vendor (utility company). The vendor_type column
    has a CHECK constraint on Electric / Gas / Both / Other, so we map
    our internal utility_type strings into one of those.
    """
    if not vendor_name:
        vendor_name = "Unknown Vendor"

    v_type = normalise_vendor_type(utility_type)

    cur = conn.cursor()
    cur.execute(
        "SELECT vendor_id FROM Vendors WHERE vendor_name = ?",
        (vendor_name,)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute(
        "INSERT INTO Vendors (vendor_name, vendor_type) VALUES (?, ?)",
        (vendor_name, v_type)
    )
    return cur.lastrowid


def get_or_create_account(conn, property_id, vendor_id, fields):
    """
    Find or create the account (one per meter at a property).
    Account number is the natural key (UNIQUE constraint at the schema
    level) — duplicate account numbers get unified into one account_id.
    """
    acc_num = fields.get("account_number")
    if not acc_num:
        return None

    cur = conn.cursor()
    cur.execute(
        "SELECT account_id FROM Accounts WHERE account_number = ?",
        (acc_num,)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("""
        INSERT INTO Accounts (property_id, vendor_id, account_number,
                              meter_number, rate_code)
        VALUES (?, ?, ?, ?, ?)
    """, (
        property_id,
        vendor_id,
        acc_num,
        fields.get("meter_number"),
        fields.get("rate_code") or fields.get("tariff_code"),
    ))
    return cur.lastrowid


# ─────────────────────────────────────────────────────────────────────────────
# Main save function
# ─────────────────────────────────────────────────────────────────────────────

def save_bill_to_db(result: dict,
                     db_path: str = "troy_banks_relational.db") -> bool:
    """
    Saves an extracted bill into the relational schema.

    Returns True on success, False on duplicate or error.

    Hierarchy auto-creation: client → property → vendor → account
    each get created on first encounter. Subsequent bills from the
    same source link to existing rows.

    Duplicate detection: enforced by (account_id, billing_date).
    Trying to save the same bill twice returns False without raising.

    Constraint normalisation: all CHECK-constrained values
    (utility_type, usage_unit, vendor_type, line_item.category) are
    normalised before insert so free-text output from the model doesn't
    fail at the database layer.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    fields = result.get("extracted_fields", {})

    try:
        # ── 1. Resolve the parent hierarchy ──────────────────────────────
        client_id = get_or_create_client(
            conn,
            fields.get("client_name") or fields.get("customer_name")
        )
        prop_id   = get_or_create_property(conn, client_id, fields)
        vendor_id = get_or_create_vendor(
            conn,
            fields.get("vendor_name") or fields.get("provider_name"),
            fields.get("utility_type"),
        )
        acc_id    = get_or_create_account(conn, prop_id, vendor_id, fields)

        if not acc_id:
            print("  ⚠ Failed: No account number found in extraction.")
            return False

        # ── 2. Normalise constraint-bound values ─────────────────────────
        utility_type = normalise_utility_type(fields.get("utility_type"))
        usage_unit   = normalise_usage_unit(fields.get("usage_unit"))

        billing_date = normalise_date(
            fields.get("bill_date") or fields.get("billing_date")
        )

        # ── 3. Duplicate check ───────────────────────────────────────────
        cur = conn.cursor()
        cur.execute(
            "SELECT bill_id FROM Bills "
            "WHERE account_id = ? AND billing_date = ?",
            (acc_id, billing_date)
        )
        if cur.fetchone():
            print(
                f"  ⏭ Duplicate — account={fields.get('account_number')} "
                f"date={billing_date} already exists"
            )
            return False

        # ── 4. Build the bill row ────────────────────────────────────────
        # Note: taxes_and_fees is NOT a column on Bills — it's stored as
        # a Line_Items row with category='Taxes and Surcharges'. If the
        # extraction returned taxes_and_fees as a top-level field but
        # didn't include a corresponding line item, we synthesise one.
        bill_data = {
            "account_id":           acc_id,
            "utility_type":         utility_type,
            "billing_date":         billing_date,
            "service_period_start": normalise_date(fields.get("service_period_start")),
            "service_period_end":   normalise_date(fields.get("service_period_end")),
            "due_date":             normalise_date(fields.get("due_date")),
            "total_amount":         parse_amount(
                                        fields.get("total_amount")
                                        or fields.get("amount_due")
                                    ),
            "usage_volume":         parse_amount(
                                        fields.get("usage_quantity")
                                        or fields.get("usage_volume")
                                    ),
            "usage_unit":           usage_unit,
            "demand_read":          parse_amount(fields.get("demand_read")),
            "demand_unit":          fields.get("demand_unit"),
            "rate_code":            fields.get("rate_code"),
            "tariff_code":          fields.get("tariff_code"),
            "is_anomaly_detected":  1 if fields.get("anomaly_reason") else 0,
            "anomaly_reason":       fields.get("anomaly_reason"),
            "anomaly_status":       "Unreviewed",
            "audit_timestamp":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_file":          result.get("source_file"),
        }

        # ── 5. Insert the bill ───────────────────────────────────────────
        cols         = ", ".join(bill_data.keys())
        placeholders = ", ".join("?" * len(bill_data))
        cur.execute(
            f"INSERT INTO Bills ({cols}) VALUES ({placeholders})",
            list(bill_data.values()),
        )
        bill_id = cur.lastrowid

        # ── 6. Insert line items if present ──────────────────────────────
        # Each line item has category, description, total_price.
        # Categories are normalised to one of the 9 CHECK-allowed values.
        # If the model returns a category we don't recognise, it falls
        # back to 'Other' rather than failing the save.
        line_items = fields.get("line_items") or []
        line_item_count = 0
        line_items_have_taxes = False

        for item in line_items:
            if not isinstance(item, dict):
                continue
            try:
                category = normalise_category(item.get("category"))
                if category == "Taxes and Surcharges":
                    line_items_have_taxes = True
                cur.execute("""
                    INSERT INTO Line_Items (bill_id, category,
                                            description, total_price)
                    VALUES (?, ?, ?, ?)
                """, (
                    bill_id,
                    category,
                    item.get("description"),
                    parse_amount(item.get("total_price")),
                ))
                line_item_count += 1
            except sqlite3.Error as e:
                print(f"  ⚠ Skipped malformed line item: {e}")

        # If the extraction returned a top-level taxes_and_fees value but
        # there's no Taxes line item, synthesise one. This keeps the
        # anomaly detector's per-category sums consistent — taxes_pct
        # is computed from Line_Items, so a "phantom" tax value at the
        # bill level wouldn't be picked up otherwise.
        taxes_value = parse_amount(fields.get("taxes_and_fees"))
        if taxes_value > 0 and not line_items_have_taxes:
            try:
                cur.execute("""
                    INSERT INTO Line_Items (bill_id, category,
                                            description, total_price)
                    VALUES (?, ?, ?, ?)
                """, (
                    bill_id,
                    "Taxes and Surcharges",
                    "Synthesised from taxes_and_fees field",
                    taxes_value,
                ))
                line_item_count += 1
            except sqlite3.Error as e:
                print(f"  ⚠ Couldn't synthesise tax line item: {e}")

        conn.commit()

        line_item_msg = (
            f", {line_item_count} line item(s)" if line_item_count else ""
        )
        print(
            f"  ✓ Saved bill_id={bill_id}  "
            f"account={fields.get('account_number')}  "
            f"total=${bill_data['total_amount']:.2f}"
            f"{line_item_msg}"
        )
        return True

    except Exception as e:
        print(f"  ✗ Database error: {e}")
        conn.rollback()
        return False

    finally:
        conn.close()