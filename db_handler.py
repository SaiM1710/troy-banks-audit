import re
import sqlite3
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# Date normalisation (Remains the same, but essential for SQLite)
# ─────────────────────────────────────────────────────────────────────────────

DATE_FORMATS = [
    "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", 
    "%b %d, %Y", "%b %d %Y", "%m-%d-%Y"
]

def normalise_date(value: str | None) -> str | None:
    if not value or not isinstance(value, str): return None
    value = value.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def parse_amount(value) -> float:
    if value is None: return 0.0
    if isinstance(value, (int, float)): return float(value)
    cleaned = re.sub(r"[$,\s]", "", str(value)).replace("(", "-").replace(")", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

# ─────────────────────────────────────────────────────────────────────────────
# Hierarchy Lookups (The "Fix")
# ─────────────────────────────────────────────────────────────────────────────

def get_or_create_client(conn, name):
    cur = conn.cursor()
    cur.execute("SELECT client_id FROM Clients WHERE client_name = ?", (name,))
    row = cur.fetchone()
    if row: return row[0]
    
    cur.execute("INSERT INTO Clients (client_name) VALUES (?)", (name,))
    return cur.lastrowid

def get_or_create_property(conn, client_id, fields):
    address = fields.get("address", "Unknown Address")
    cur = conn.cursor()
    cur.execute("SELECT property_id FROM Properties WHERE client_id = ? AND address = ?", (client_id, address))
    row = cur.fetchone()
    if row: return row[0]

    cur.execute("""
        INSERT INTO Properties (client_id, address, city, state, zip_code, property_name)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (client_id, address, fields.get("city"), fields.get("state"), fields.get("zip"), fields.get("property_name")))
    return cur.lastrowid

def get_or_create_vendor(conn, vendor_name, utility_type):
    # Map utility type to the Vendor CHECK constraint
    v_type = 'Other'
    if utility_type:
        if 'Electric' in utility_type: v_type = 'Electric'
        elif 'Gas' in utility_type: v_type = 'Gas'

    cur = conn.cursor()
    cur.execute("SELECT vendor_id FROM Vendors WHERE vendor_name = ?", (vendor_name,))
    row = cur.fetchone()
    if row: return row[0]

    cur.execute("INSERT INTO Vendors (vendor_name, vendor_type) VALUES (?, ?)", (vendor_name, v_type))
    return cur.lastrowid

def get_or_create_account(conn, property_id, vendor_id, fields):
    acc_num = fields.get("account_number")
    if not acc_num: return None
    
    cur = conn.cursor()
    cur.execute("SELECT account_id FROM Accounts WHERE account_number = ?", (acc_num,))
    row = cur.fetchone()
    if row: return row[0]

    cur.execute("""
        INSERT INTO Accounts (property_id, vendor_id, account_number, meter_number, rate_code)
        VALUES (?, ?, ?, ?, ?)
    """, (property_id, vendor_id, acc_num, fields.get("meter_number"), fields.get("rate_code")))
    return cur.lastrowid
def get_or_create_bill(conn, account_id, fields):
    """
    Checks if a bill for this account and date already exists.
    If not, creates the bill and inserts the tax as a line item.
    """
    # 1. Normalise the key identifier
    billing_date = normalise_date(fields.get("billing_date") or fields.get("bill_date"))
    
    if not account_id or not billing_date:
        return None
    
    cur = conn.cursor()

    # 2. Duplicate Check
    cur.execute(
        "SELECT bill_id FROM Bills WHERE account_id = ? AND billing_date = ?", 
        (account_id, billing_date)
    )
    row = cur.fetchone()
    if row:
        return row[0]

    # 3. Insert the Bill
    # Note: service_period_start and service_period_end are included here
    cur.execute("""
        INSERT INTO Bills (
            account_id, 
            utility_type, 
            billing_date, 
            service_period_start, 
            service_period_end, 
            due_date,
            total_amount, 
            usage_volume, 
            usage_unit,
            audit_timestamp,
            anomaly_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        account_id,
        fields.get("utility_type", "Unknown"),
        billing_date,
        normalise_date(fields.get("service_period_start")),
        normalise_date(fields.get("service_period_end")),
        normalise_date(fields.get("due_date")),
        parse_amount(fields.get("total_amount")),
        parse_amount(fields.get("usage_volume")),
        fields.get("usage_unit", "Unknown"),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Unreviewed"
    ))
    
    bill_id = cur.lastrowid

    # 4. Insert Tax into Line_Items (linked to the bill we just created)
    tax_value = parse_amount(fields.get("tax"))
    if tax_value != 0:
        cur.execute("""
            INSERT INTO Line_Items (bill_id, category, description, total_price)
            VALUES (?, 'Taxes and Surcharges', 'Total Tax Calculated', ?)
        """, (bill_id, tax_value))

    return bill_id
# ─────────────────────────────────────────────────────────────────────────────
# Main Save Function
# ─────────────────────────────────────────────────────────────────────────────

def save_bill_to_db(result: dict, db_path: str = "troy_banks_relational.db") -> bool:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    fields = result.get("extracted_fields", {})
    
    try:
        # 1. Resolve Hierarchy
        client_id = get_or_create_client(conn, fields.get("client_name") or fields.get("customer_name"))
        prop_id   = get_or_create_property(conn, client_id, fields)
        vendor_id = get_or_create_vendor(conn, fields.get("vendor_name") or fields.get("provider_name"), fields.get("utility_type"))
        acc_id    = get_or_create_account(conn, prop_id, vendor_id, fields)
        bill_table = get_or_create_bill(conn, fields.get("service_period_start"), fields.get("service_period_end"), fields.get("tax"))
        if not acc_id:
            print("  ⚠ Failed: No account number found.")
            return False

        # 2. Prepare Bill Data (Matching your Schema exactly)
        billing_date = normalise_date(fields.get("bill_date") or fields.get("billing_date"))
        
        # Duplicate Check
        cur = conn.cursor()
        cur.execute("SELECT bill_id FROM Bills WHERE account_id = ? AND billing_date = ?", (acc_id, billing_date))
        if cur.fetchone():
            print(f"  ⏭ Duplicate bill for account {fields.get('account_number')} on {billing_date}")
            return False

        bill_data = {
            "account_id":           acc_id,
            "utility_type":         fields.get("utility_type", "Unknown"),
            "billing_date":         billing_date,
            # "service_period_start": normalise_date(fields.get("service_period_start")),
            # "service_period_end":   normalise_date(fields.get("service_period_end")),
            "service_period_start": bill_table,
            "service_period_end": bill_table,
            "due_date":             normalise_date(fields.get("due_date")),
            "total_amount":         parse_amount(fields.get("total_amount") or fields.get("amount_due")),
            "usage_volume":         parse_amount(fields.get("usage_quantity") or fields.get("usage_volume")),
            "usage_unit":           fields.get("usage_unit", "Unknown"),
            "demand_read":          parse_amount(fields.get("demand_read")),
            "demand_unit":          fields.get("demand_unit"),
            "rate_code":            fields.get("rate_code"),
            "tariff_code":          fields.get("tariff_code"),
            "is_anomaly_detected":  1 if fields.get("anomaly_reason") else 0,
            "anomaly_reason":       fields.get("anomaly_reason"),
            "anomaly_status":       "Unreviewed",
            "audit_timestamp":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_file":          result.get("source_file")
        }

        # 3. Insert Bill
        cols = ", ".join(bill_data.keys())
        placeholders = ", ".join("?" * len(bill_data))
        cur.execute(f"INSERT INTO Bills ({cols}) VALUES ({placeholders})", list(bill_data.values()))
        bill_id = cur.lastrowid

        # 4. Handle Line Items (If your extraction includes them)
        line_items = fields.get("line_items", [])
        for item in line_items:
            cur.execute("""
                INSERT INTO Line_Items (bill_id, category, description, total_price)
                VALUES (?, ?, ?, ?)
            """, (bill_id, item.get("category", "Other"), item.get("description"), parse_amount(item.get("total_price"))))

        conn.commit()
        print(f"  ✓ Saved bill_id={bill_id} for {fields.get('account_number')}")
        return True

    except Exception as e:
        print(f"  ✗ Database error: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()