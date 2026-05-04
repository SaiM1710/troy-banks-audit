"""
db_handler.py — saves extracted bill data into the Troy & Banks
relational database.

Schema this writes to:
  Clients ──┬── Properties ──┬── Accounts ──┬── Bills ── Line_Items
  Vendors ─────────────────────┘                       │
                                                        │
                                                        └── Anomaly_Analysis

Each save resolves the full hierarchy (client → property → vendor →
account) before inserting the bill, auto-creating any missing rows.
Bills carry their own taxes_and_fees, service period dates, and
tariff code so the anomaly detector can run charge-composition and
weather-normalised analyses without joining to line items.

The line_items array on the extraction result gets exploded into the
Line_Items table so the anomaly detector can compute per-category
proportions (delivery_pct, supply_pct, demand_pct, taxes_pct).
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
    Handles "$1,234.56", "(50.00)" for negatives, plain numbers,
    and Python None. Returns 0.0 on garbage so the database insert
    doesn't fail with NULL on a NOT NULL column.
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
    address = fields.get("service_address") or fields.get("address") or "Unknown Address"

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
    Find or create the vendor (utility company). The vendor_type
    column has a CHECK constraint on Electric / Gas / Other, so we
    map our internal utility_type strings into one of those.
    """
    if not vendor_name:
        vendor_name = "Unknown Vendor"

    # Map internal utility_type strings to the vendor_type CHECK constraint
    v_type = "Other"
    if utility_type:
        ut_lower = str(utility_type).lower()
        if "electric" in ut_lower:
            v_type = "Electric"
        elif "gas" in ut_lower:
            v_type = "Gas"

    cur = conn.cursor()
    cur.execute("SELECT vendor_id FROM Vendors WHERE vendor_name = ?", (vendor_name,))
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
    Account number is the natural key — duplicate account numbers
    from the same vendor get unified into one account_id.
    """
    acc_num = fields.get("account_number")
    if not acc_num:
        return None

    cur = conn.cursor()
    cur.execute("SELECT account_id FROM Accounts WHERE account_number = ?", (acc_num,))
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

    The result dict is expected to have the shape produced by app1's
    row_to_db_result() wrapper — i.e. fields under "extracted_fields"
    and metadata at the top level.

    Hierarchy auto-creation: if this is the first bill from a particular
    client/property/vendor/account, the parent rows are created on the
    fly. Subsequent bills from the same source link to the existing rows.

    Duplicate detection: enforced by (account_id, billing_date). Trying
    to save the same bill twice returns False without raising.

    Line items: if fields["line_items"] is a non-empty list, each entry
    gets exploded into the Line_Items table. Categories should already
    be normalised to one of: Delivery Charge, Supply Charge, Demand
    Charge, Taxes and Surcharges, Other.
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

        # ── 2. Build the bill row ────────────────────────────────────────
        billing_date = normalise_date(
            fields.get("bill_date") or fields.get("billing_date")
        )

        # Duplicate check before insert — clearer error message than
        # waiting for a UNIQUE constraint violation
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

        # taxes_and_fees can come either as a directly extracted field OR
        # be derived by summing line items in the "Taxes and Surcharges"
        # category. Prefer the explicit field if present.
        taxes_value = fields.get("taxes_and_fees")
        if taxes_value is None and fields.get("line_items"):
            taxes_value = sum(
                parse_amount(li.get("total_price"))
                for li in fields["line_items"]
                if li.get("category") == "Taxes and Surcharges"
            )

        bill_data = {
            "account_id":           acc_id,
            "utility_type":         fields.get("utility_type", "Unknown"),
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
            "usage_unit":           fields.get("usage_unit") or "Unknown",
            "demand_read":          parse_amount(fields.get("demand_read")),
            "demand_unit":          fields.get("demand_unit"),
            "rate_code":            fields.get("rate_code"),
            "tariff_code":          fields.get("tariff_code"),
            # New top-level field — sum of all taxes/surcharges on this bill
            "taxes_and_fees":       parse_amount(taxes_value),
            "is_anomaly_detected":  1 if fields.get("anomaly_reason") else 0,
            "anomaly_reason":       fields.get("anomaly_reason"),
            "anomaly_status":       "Unreviewed",
            "audit_timestamp":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_file":          result.get("source_file"),
        }

        # ── 3. Insert the bill ───────────────────────────────────────────
        cols         = ", ".join(bill_data.keys())
        placeholders = ", ".join("?" * len(bill_data))
        cur.execute(
            f"INSERT INTO Bills ({cols}) VALUES ({placeholders})",
            list(bill_data.values()),
        )
        bill_id = cur.lastrowid

        # ── 4. Insert line items if present ──────────────────────────────
        # Each line item has category, description, total_price.
        # Categories are constrained at the extraction layer to one of
        # Delivery Charge / Supply Charge / Demand Charge /
        # Taxes and Surcharges / Other so the anomaly detector can
        # compute proportions reliably.
        line_items = fields.get("line_items") or []
        line_item_count = 0
        for item in line_items:
            if not isinstance(item, dict):
                continue
            try:
                cur.execute("""
                    INSERT INTO Line_Items (bill_id, category,
                                            description, total_price)
                    VALUES (?, ?, ?, ?)
                """, (
                    bill_id,
                    item.get("category", "Other"),
                    item.get("description"),
                    parse_amount(item.get("total_price")),
                ))
                line_item_count += 1
            except sqlite3.Error as e:
                print(f"  ⚠ Skipped malformed line item: {e}")

        conn.commit()

        line_item_msg = f", {line_item_count} line item(s)" if line_item_count else ""
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