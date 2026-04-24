import sqlite3
import random
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

DB_NAME = "troy_banks_relational.db"

# ── SEED DATA DEFINITIONS ──────────────────────────────────────────────────

VENDORS = [
    ("National Grid",  "Both"),
    ("ConEdison",      "Both"),
    ("KeySpan Energy", "Gas"),
    ("RG&E",           "Both"),
]

CLIENTS = [
    {
        "name":    "Buffalo General Hospital",
        "industry": "Healthcare",
        "contact": "James Whitfield",
        "email":   "jwhitfield@buffgeneral.org",
        "phone":   "716-555-0101",
        "properties": [
            {"address": "100 High St",       "city": "Buffalo",     "state": "NY", "zip": "14203", "name": "Main Campus"},
            {"address": "250 Maple Ave",     "city": "Cheektowaga", "state": "NY", "zip": "14225", "name": "Outpatient Center"},
        ]
    },
    {
        "name":    "Great Lakes Manufacturing",
        "industry": "Manufacturing",
        "contact": "Sandra Kowalski",
        "email":   "skowalski@greatlakesmfg.com",
        "phone":   "716-555-0202",
        "properties": [
            {"address": "500 Industrial Pkwy", "city": "Lackawanna", "state": "NY", "zip": "14218", "name": "Plant A"},
            {"address": "750 Factory Rd",      "city": "Tonawanda",  "state": "NY", "zip": "14150", "name": "Plant B"},
        ]
    },
    {
        "name":    "Sunrise Restaurant Group",
        "industry": "Food Service",
        "contact": "Michael Torres",
        "email":   "mtorres@sunrisegroup.com",
        "phone":   "716-555-0303",
        "properties": [
            {"address": "1200 Transit Rd",   "city": "Amherst",    "state": "NY", "zip": "14221", "name": "Amherst Location"},
            {"address": "890 Niagara Falls Blvd", "city": "Tonawanda", "state": "NY", "zip": "14150", "name": "Tonawanda Location"},
            {"address": "340 Delaware Ave",  "city": "Buffalo",    "state": "NY", "zip": "14202", "name": "Downtown Location"},
            {"address": "2100 Sheridan Dr",  "city": "Kenmore",    "state": "NY", "zip": "14223", "name": "Kenmore Location"},
            {"address": "670 Dick Rd",       "city": "Depew",      "state": "NY", "zip": "14043", "name": "Depew Location"},
        ]
    },
    {
        "name":    "Westfield Central School District",
        "industry": "Education",
        "contact": "Patricia Nguyen",
        "email":   "pnguyen@westfieldcsd.edu",
        "phone":   "716-555-0404",
        "properties": [
            {"address": "20 E Main St",      "city": "Westfield",  "state": "NY", "zip": "14787", "name": "Main School"},
            {"address": "45 Prospect St",    "city": "Westfield",  "state": "NY", "zip": "14787", "name": "Annex Building"},
        ]
    },
    {
        "name":    "Empire Retail Partners",
        "industry": "Retail",
        "contact": "David Chen",
        "email":   "dchen@empirepartners.com",
        "phone":   "716-555-0505",
        "properties": [
            {"address": "3500 McKinley Pkwy", "city": "Hamburg",   "state": "NY", "zip": "14075", "name": "Hamburg Store"},
            {"address": "1800 Walden Ave",    "city": "Cheektowaga","state": "NY", "zip": "14225", "name": "Cheektowaga Store"},
            {"address": "4545 Transit Rd",    "city": "Lancaster",  "state": "NY", "zip": "14086", "name": "Lancaster Store"},
        ]
    },
]

# Commercial electric billing profiles per industry (monthly kWh, base charges)
ELECTRIC_PROFILES = {
    "Healthcare":    {"kwh_base": 85000,  "kwh_variance": 15000, "fixed": 45.00, "delivery_rate": 0.068, "supply_rate": 0.055, "demand_kw": 180, "demand_rate": 12.50},
    "Manufacturing": {"kwh_base": 250000, "kwh_variance": 50000, "fixed": 95.00, "delivery_rate": 0.052, "supply_rate": 0.048, "demand_kw": 650, "demand_rate": 11.00},
    "Food Service":  {"kwh_base": 12000,  "kwh_variance": 3000,  "fixed": 22.00, "delivery_rate": 0.079, "supply_rate": 0.063, "demand_kw": 45,  "demand_rate": 14.00},
    "Education":     {"kwh_base": 35000,  "kwh_variance": 12000, "fixed": 35.00, "delivery_rate": 0.071, "supply_rate": 0.058, "demand_kw": 90,  "demand_rate": 13.00},
    "Retail":        {"kwh_base": 18000,  "kwh_variance": 4000,  "fixed": 28.00, "delivery_rate": 0.076, "supply_rate": 0.061, "demand_kw": 60,  "demand_rate": 13.50},
}

# Commercial gas billing profiles per industry (monthly Therms)
GAS_PROFILES = {
    "Healthcare":    {"therms_base": 4500, "therms_variance": 800,  "fixed": 38.00, "delivery_rate": 0.42, "supply_rate": 0.68},
    "Manufacturing": {"therms_base": 8000, "therms_variance": 2000, "fixed": 75.00, "delivery_rate": 0.38, "supply_rate": 0.62},
    "Food Service":  {"therms_base": 800,  "therms_variance": 200,  "fixed": 18.00, "delivery_rate": 0.48, "supply_rate": 0.72},
    "Education":     {"therms_base": 2500, "therms_variance": 1000, "fixed": 32.00, "delivery_rate": 0.44, "supply_rate": 0.65},
    "Retail":        {"therms_base": 600,  "therms_variance": 150,  "fixed": 16.00, "delivery_rate": 0.46, "supply_rate": 0.70},
}

# Seasonal multipliers — commercial bills spike in winter/summer
SEASONAL_ELECTRIC = {1:1.3, 2:1.25, 3:1.0, 4:0.85, 5:0.9, 6:1.2, 7:1.4, 8:1.35, 9:1.1, 10:0.9, 11:1.0, 12:1.2}
SEASONAL_GAS      = {1:2.8, 2:2.6, 3:1.8, 4:1.2, 5:0.7, 6:0.4, 7:0.3, 8:0.3, 9:0.6, 10:1.1, 11:1.9, 12:2.5}

RATE_CODES_ELECTRIC = {
    "Healthcare":    "SC-3A Large Commercial",
    "Manufacturing": "SC-4 Industrial",
    "Food Service":  "SC-2 Small Commercial",
    "Education":     "SC-3 Medium Commercial",
    "Retail":        "SC-2 Small Commercial",
}

RATE_CODES_GAS = {
    "Healthcare":    "GC-3 Large Commercial Gas",
    "Manufacturing": "GC-4 Industrial Gas",
    "Food Service":  "GC-2 Small Commercial Gas",
    "Education":     "GC-3 Medium Commercial Gas",
    "Retail":        "GC-2 Small Commercial Gas",
}


def generate_account_number(seed: int) -> str:
    random.seed(seed)
    return f"{random.randint(10000000, 99999999)}"


def generate_meter_number(seed: int) -> str:
    random.seed(seed + 9999)
    return f"{random.randint(10000000, 99999999)}"


def generate_electric_bill(
    account_id: int,
    bill_date: date,
    industry: str,
    introduce_anomaly: bool = False
) -> dict:
    profile = ELECTRIC_PROFILES[industry]
    month = bill_date.month
    seasonal = SEASONAL_ELECTRIC[month]

    # Usage
    kwh = int((profile["kwh_base"] + random.randint(-profile["kwh_variance"], profile["kwh_variance"])) * seasonal)
    demand_kw = round(profile["demand_kw"] * seasonal * random.uniform(0.85, 1.15), 1)

    # Charges
    fixed    = profile["fixed"]
    delivery = round(kwh * profile["delivery_rate"], 2)
    supply   = round(kwh * profile["supply_rate"], 2)
    demand   = round(demand_kw * profile["demand_rate"], 2)
    taxes    = round((fixed + delivery + supply + demand) * 0.048, 2)
    total    = round(fixed + delivery + supply + demand + taxes, 2)

    anomaly_reason = ""

    if introduce_anomaly:
        anomaly_type = random.choice(["math_error", "demand_spike", "double_charge"])

        if anomaly_type == "math_error":
            # Inflate total without changing sub-charges
            total = round(total * random.uniform(1.08, 1.20), 2)
            anomaly_reason = (
                f"MATH AUDIT FAILED: Sub-charges sum to "
                f"${fixed+delivery+supply+demand+taxes:.2f} but total due is "
                f"${total:.2f}. Discrepancy of "
                f"${abs(total-(fixed+delivery+supply+demand+taxes)):.2f}."
            )

        elif anomaly_type == "demand_spike":
            # Spike demand charge by 3x
            demand = round(demand * 3.0, 2)
            total  = round(fixed + delivery + supply + demand + taxes, 2)
            anomaly_reason = (
                f"MATH AUDIT FAILED: Demand charge of ${demand:.2f} is "
                f"abnormally high — 3x normal level for this account."
            )

        elif anomaly_type == "double_charge":
            # Double the fixed charge
            fixed  = round(fixed * 2.0, 2)
            total  = round(fixed + delivery + supply + demand + taxes, 2)
            anomaly_reason = (
                f"MATH AUDIT FAILED: Fixed monthly charge of ${fixed:.2f} "
                f"appears to be double billed."
            )

    service_start = bill_date - relativedelta(months=1)
    service_end   = bill_date - relativedelta(days=1)

    return {
        "account_id":           account_id,
        "utility_type":         "Electric",
        "billing_date":         bill_date.strftime("%Y-%m-%d"),
        "service_period_start": service_start.strftime("%Y-%m-%d"),
        "service_period_end":   service_end.strftime("%Y-%m-%d"),
        "total_amount":         total,
        "usage_volume":         float(kwh),
        "usage_unit":           "kWh",
        "demand_read":          demand_kw,
        "demand_unit":          "kW",
        "rate_code":            RATE_CODES_ELECTRIC[industry],
        "tariff_code":          f"ELEC-{bill_date.year}",
        "is_anomaly_detected":  1 if introduce_anomaly else 0,
        "anomaly_reason":       anomaly_reason,
        "anomaly_status":       "Unreviewed" if introduce_anomaly else "Unreviewed",
        "audit_timestamp":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_file":          f"synthetic_electric_{bill_date.strftime('%Y%m')}.pdf",
        # Line items
        "fixed":                fixed,
        "delivery":             delivery,
        "supply":               supply,
        "demand":               demand,
        "taxes":                taxes,
    }


def generate_gas_bill(
    account_id: int,
    bill_date: date,
    industry: str,
    introduce_anomaly: bool = False
) -> dict:
    profile = GAS_PROFILES[industry]
    month = bill_date.month
    seasonal = SEASONAL_GAS[month]

    # Skip gas bills in summer for restaurants and retail
    if industry in ("Food Service", "Retail") and month in (6, 7, 8):
        therms = int(profile["therms_base"] * 0.15)
    else:
        therms = int((profile["therms_base"] + random.randint(-profile["therms_variance"], profile["therms_variance"])) * seasonal)

    therms = max(therms, 10)

    fixed    = profile["fixed"]
    delivery = round(therms * profile["delivery_rate"], 2)
    supply   = round(therms * profile["supply_rate"], 2)
    taxes    = round((fixed + delivery + supply) * 0.045, 2)
    total    = round(fixed + delivery + supply + taxes, 2)

    anomaly_reason = ""

    if introduce_anomaly:
        anomaly_type = random.choice(["math_error", "usage_spike"])

        if anomaly_type == "math_error":
            total = round(total * random.uniform(1.05, 1.15), 2)
            anomaly_reason = (
                f"MATH AUDIT FAILED: Sub-charges sum to "
                f"${fixed+delivery+supply+taxes:.2f} but total due is "
                f"${total:.2f}. Discrepancy of "
                f"${abs(total-(fixed+delivery+supply+taxes)):.2f}."
            )

        elif anomaly_type == "usage_spike":
            therms = int(therms * 2.8)
            delivery = round(therms * profile["delivery_rate"], 2)
            supply   = round(therms * profile["supply_rate"], 2)
            taxes    = round((fixed + delivery + supply) * 0.045, 2)
            total    = round(fixed + delivery + supply + taxes, 2)
            anomaly_reason = (
                f"MATH AUDIT FAILED: Gas usage of {therms} Therms is "
                f"abnormally high — nearly 3x normal for this account and season."
            )

    service_start = bill_date - relativedelta(months=1)
    service_end   = bill_date - relativedelta(days=1)

    return {
        "account_id":           account_id,
        "utility_type":         "Gas",
        "billing_date":         bill_date.strftime("%Y-%m-%d"),
        "service_period_start": service_start.strftime("%Y-%m-%d"),
        "service_period_end":   service_end.strftime("%Y-%m-%d"),
        "total_amount":         total,
        "usage_volume":         float(therms),
        "usage_unit":           "Therms",
        "demand_read":          0.0,
        "demand_unit":          "",
        "rate_code":            RATE_CODES_GAS[industry],
        "tariff_code":          f"GAS-{bill_date.year}",
        "is_anomaly_detected":  1 if introduce_anomaly else 0,
        "anomaly_reason":       anomaly_reason,
        "anomaly_status":       "Unreviewed" if introduce_anomaly else "Unreviewed",
        "audit_timestamp":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_file":          f"synthetic_gas_{bill_date.strftime('%Y%m')}.pdf",
        "fixed":                fixed,
        "delivery":             delivery,
        "supply":               supply,
        "demand":               0.0,
        "taxes":                taxes,
    }


def insert_bill_direct(cursor, bill: dict) -> int:
    cursor.execute('''
        INSERT INTO Bills (
            account_id, utility_type, billing_date,
            service_period_start, service_period_end,
            total_amount, usage_volume, usage_unit,
            demand_read, demand_unit, rate_code, tariff_code,
            is_anomaly_detected, anomaly_reason, anomaly_status,
            audit_timestamp, source_file
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        bill["account_id"], bill["utility_type"], bill["billing_date"],
        bill["service_period_start"], bill["service_period_end"],
        bill["total_amount"], bill["usage_volume"], bill["usage_unit"],
        bill["demand_read"], bill["demand_unit"], bill["rate_code"], bill["tariff_code"],
        bill["is_anomaly_detected"], bill["anomaly_reason"], bill["anomaly_status"],
        bill["audit_timestamp"], bill["source_file"]
    ))

    bill_id = cursor.lastrowid

    line_items = [
        ("Fixed Monthly Charge", bill["fixed"]),
        ("Delivery Charge",      bill["delivery"]),
        ("Supply Charge",        bill["supply"]),
        ("Demand Charge",        bill["demand"]),
        ("Taxes and Surcharges", bill["taxes"]),
    ]

    for category, price in line_items:
        if price != 0.0:
            cursor.execute(
                "INSERT INTO Line_Items (bill_id, category, total_price) VALUES (?, ?, ?)",
                (bill_id, category, price)
            )

    return bill_id


def seed():
    random.seed(42)

    conn = sqlite3.connect(DB_NAME, timeout=10.0)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    print("\n[SEEDER] Starting synthetic data generation...")
    print("=" * 55)

    # Insert vendors
    vendor_ids = {}
    for vendor_name, vendor_type in VENDORS:
        cursor.execute(
            "INSERT OR IGNORE INTO Vendors (vendor_name, vendor_type) VALUES (?, ?)",
            (vendor_name, vendor_type)
        )
        cursor.execute(
            "SELECT vendor_id FROM Vendors WHERE vendor_name = ?",
            (vendor_name,)
        )
        vendor_ids[vendor_name] = cursor.fetchone()[0]
    print(f"[SEEDER] Vendors inserted: {list(vendor_ids.keys())}")

    total_bills    = 0
    total_anomalies = 0
    account_seed   = 1000

    for client_data in CLIENTS:
        # Insert client
        cursor.execute('''
            INSERT OR IGNORE INTO Clients
            (client_name, industry, contact_name, contact_email, contact_phone, date_onboarded, is_active)
            VALUES (?, ?, ?, ?, ?, ?, 1)
        ''', (
            client_data["name"], client_data["industry"],
            client_data["contact"], client_data["email"],
            client_data["phone"], "2019-01-01"
        ))
        cursor.execute(
            "SELECT client_id FROM Clients WHERE client_name = ?",
            (client_data["name"],)
        )
        client_id = cursor.fetchone()[0]
        print(f"\n[SEEDER] Client: {client_data['name']} (ID: {client_id})")

        for prop in client_data["properties"]:
            # Insert property
            cursor.execute('''
                INSERT INTO Properties
                (client_id, address, city, state, zip_code, property_name)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                client_id, prop["address"], prop["city"],
                prop["state"], prop["zip"], prop["name"]
            ))
            cursor.execute(
                "SELECT property_id FROM Properties WHERE address = ? AND client_id = ?",
                (prop["address"], client_id)
            )
            property_id = cursor.fetchone()[0]

            # Assign vendor based on location
            vendor_name = random.choice(list(vendor_ids.keys()))
            vendor_id   = vendor_ids[vendor_name]

            # Create Electric account
            elec_account_num = generate_account_number(account_seed)
            account_seed += 1
            cursor.execute('''
                INSERT OR IGNORE INTO Accounts
                (property_id, vendor_id, account_number, meter_number, rate_code)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                property_id, vendor_id, elec_account_num,
                generate_meter_number(account_seed),
                RATE_CODES_ELECTRIC[client_data["industry"]]
            ))
            cursor.execute(
                "SELECT account_id FROM Accounts WHERE account_number = ?",
                (elec_account_num,)
            )
            elec_account_id = cursor.fetchone()[0]

            # Create Gas account
            gas_account_num = generate_account_number(account_seed)
            account_seed += 1
            cursor.execute('''
                INSERT OR IGNORE INTO Accounts
                (property_id, vendor_id, account_number, meter_number, rate_code)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                property_id, vendor_id, gas_account_num,
                generate_meter_number(account_seed),
                RATE_CODES_GAS[client_data["industry"]]
            ))
            cursor.execute(
                "SELECT account_id FROM Accounts WHERE account_number = ?",
                (gas_account_num,)
            )
            gas_account_id = cursor.fetchone()[0]

            print(f"  Property: {prop['name']} | Vendor: {vendor_name}")
            print(f"  Electric Account: {elec_account_num} | Gas Account: {gas_account_num}")

            # Generate 6 years of monthly bills (2019-2024)
            start_date = date(2019, 1, 1)
            end_date   = date(2024, 12, 1)
            current    = start_date
            prop_bills = 0
            prop_anomalies = 0

            while current <= end_date:
                # 15% chance of anomaly on any bill
                elec_anomaly = random.random() < 0.15
                gas_anomaly  = random.random() < 0.15

                # Generate and insert electric bill
                elec_bill = generate_electric_bill(
                    elec_account_id, current,
                    client_data["industry"], elec_anomaly
                )
                insert_bill_direct(cursor, elec_bill)
                prop_bills += 1
                if elec_anomaly:
                    prop_anomalies += 1

                # Generate and insert gas bill
                gas_bill = generate_gas_bill(
                    gas_account_id, current,
                    client_data["industry"], gas_anomaly
                )
                insert_bill_direct(cursor, gas_bill)
                prop_bills += 1
                if gas_anomaly:
                    prop_anomalies += 1

                current += relativedelta(months=1)

            print(f"  Bills generated: {prop_bills} | Anomalies: {prop_anomalies}")
            total_bills     += prop_bills
            total_anomalies += prop_anomalies

    conn.commit()
    conn.close()

    print("\n" + "=" * 55)
    print(f"[SEEDER] COMPLETE")
    print(f"[SEEDER] Total bills inserted:    {total_bills}")
    print(f"[SEEDER] Total anomalies seeded:  {total_anomalies}")
    print(f"[SEEDER] Clients:                 {len(CLIENTS)}")
    print(f"[SEEDER] Ready for MCP server.")
    print("=" * 55)


if __name__ == "__main__":
    seed()