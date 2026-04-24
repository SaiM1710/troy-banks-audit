import sqlite3
from datetime import datetime

DB_NAME = "troy_banks_relational.db"

def get_or_create_vendor(cursor, provider_name):
    cursor.execute("INSERT OR IGNORE INTO Vendors (vendor_name) VALUES (?)", (provider_name,))
    cursor.execute("SELECT vendor_id FROM Vendors WHERE vendor_name = ?", (provider_name,))
    row = cursor.fetchone()
    return row[0] if row else None

def get_or_create_account(cursor, vendor_id, account_num):
    cursor.execute(
        "INSERT OR IGNORE INTO Accounts (vendor_id, account_number) VALUES (?, ?)",
        (vendor_id, account_num)
    )
    cursor.execute("SELECT account_id FROM Accounts WHERE account_number = ?", (account_num,))
    row = cursor.fetchone()
    return row[0] if row else None

def insert_bill(parsed_data: dict, source_file: str = None) -> bool:
    """
    Inserts a fully parsed and audited bill into the database.
    Aligned to the industry-grade schema.
    """
    try:
        conn = sqlite3.connect(DB_NAME, timeout=10.0)
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()

        # --- Extract all fields safely ---
        provider     = str(parsed_data.get('provider_name', 'UNKNOWN'))
        account_num  = str(parsed_data.get('account_number', 'UNKNOWN'))
        utility_type = str(parsed_data.get('utility_type', 'Unknown'))
        stmt_date    = str(parsed_data.get('statement_date', ''))
        period_start = str(parsed_data.get('service_period_start', ''))
        period_end   = str(parsed_data.get('service_period_end', ''))
        reason       = str(parsed_data.get('anomaly_reason', ''))
        rate_code    = str(parsed_data.get('rate_code', ''))
        tariff_code  = str(parsed_data.get('tariff_code', ''))
        usage_unit   = str(parsed_data.get('usage_unit', 'Unknown'))

        total_amount = float(parsed_data.get('total_amount_due', 0.0))
        fixed        = float(parsed_data.get('fixed_monthly_charge', 0.0))
        delivery     = float(parsed_data.get('delivery_charge', 0.0))
        supply       = float(parsed_data.get('supply_charge', 0.0))
        taxes        = float(parsed_data.get('taxes_and_surcharges', 0.0))
        demand       = float(parsed_data.get('demand_charge', 0.0))
        usage_volume = float(parsed_data.get('usage_volume', 0.0))
        demand_read  = float(parsed_data.get('demand_read', 0.0))

        is_anom = 1 if parsed_data.get('is_anomaly_detected') else 0

        # Validate utility_type against allowed values
        if utility_type not in ('Electric', 'Gas'):
            utility_type = 'Unknown'

        # Validate usage_unit against allowed values
        if usage_unit not in ('kWh', 'Therms', 'CCF'):
            usage_unit = 'Unknown'

        # --- Step 1: Get or create Vendor ---
        vendor_id = get_or_create_vendor(cursor, provider)
        if vendor_id is None:
            print(f"[DB ERROR] Could not resolve vendor: {provider}")
            return False

        # --- Step 2: Get or create Account ---
        account_id = get_or_create_account(cursor, vendor_id, account_num)
        if account_id is None:
            print(f"[DB ERROR] Could not resolve account: {account_num}")
            return False
        
        if not provider or provider == 'UNKNOWN':
            print("[DB REJECTED] Missing provider name. Skipping insert.")
            return False

        if not account_num or len(account_num) < 5 or not account_num.isdigit():
            print(f"[DB REJECTED] Invalid account number: '{account_num}'. Skipping insert.")
            return False

        if total_amount <= 0.0:
            print(f"[DB REJECTED] Invalid total amount: {total_amount}. Skipping insert.")
            return False

        # --- Step 3: Insert Bill ---
        cursor.execute('''
            INSERT INTO Bills (
                account_id, utility_type, billing_date,
                service_period_start, service_period_end,
                total_amount, usage_volume, usage_unit,
                demand_read, rate_code, tariff_code,
                is_anomaly_detected, anomaly_reason, anomaly_status,
                audit_timestamp, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            account_id, utility_type, stmt_date,
            period_start, period_end,
            total_amount, usage_volume, usage_unit,
            demand_read, rate_code, tariff_code,
            is_anom, reason, 'Unreviewed',
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            source_file
        ))

        bill_id = cursor.lastrowid

        # --- Step 4: Insert Line Items ---
        # FIXED: use != 0.0 so negative credits are also stored
        line_items = [
            ('Fixed Monthly Charge', fixed),
            ('Delivery Charge',      delivery),
            ('Supply Charge',        supply),
            ('Demand Charge',        demand),
            ('Taxes and Surcharges', taxes),
            ('Credit',               -abs(float(parsed_data.get('credits', 0.0)))),
        ]

        for category, price in line_items:
            if price != 0.0:
                cursor.execute(
                    "INSERT INTO Line_Items (bill_id, category, total_price) VALUES (?, ?, ?)",
                    (bill_id, category, price)
                )

        conn.commit()
        conn.close()
        print(f"\n[DATABASE] SUCCESS: Bill inserted for account {account_num} | Bill ID: {bill_id}")
        return True

    except Exception as e:
        print(f"\n[DATABASE] FATAL ERROR: {e}")
        return False