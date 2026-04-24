import os
import sqlite3

DB_NAME = "troy_banks_relational.db"

def force_delete_locked_db():
    files_to_nuke = [DB_NAME, f"{DB_NAME}-journal", f"{DB_NAME}-wal", f"{DB_NAME}-shm"]
    for file in files_to_nuke:
        try:
            if os.path.exists(file):
                os.remove(file)
        except Exception:
            pass

def build_tables(cursor):
    cursor.execute('''CREATE TABLE Clients (
        client_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        client_name    TEXT    NOT NULL UNIQUE,
        industry       TEXT,
        contact_name   TEXT,
        contact_email  TEXT,
        contact_phone  TEXT,
        date_onboarded TEXT,
        is_active      INTEGER NOT NULL DEFAULT 1
    )''')

    cursor.execute('''CREATE TABLE Properties (
        property_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id     INTEGER NOT NULL,
        address       TEXT    NOT NULL,
        city          TEXT,
        state         TEXT,
        zip_code      TEXT,
        property_name TEXT,
        FOREIGN KEY(client_id) REFERENCES Clients(client_id)
    )''')

    cursor.execute('''CREATE TABLE Vendors (
        vendor_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        vendor_name TEXT    NOT NULL UNIQUE,
        vendor_type TEXT    CHECK(vendor_type IN ('Electric', 'Gas', 'Both', 'Other'))
    )''')

    cursor.execute('''CREATE TABLE Accounts (
        account_id     INTEGER PRIMARY KEY AUTOINCREMENT,
        property_id    INTEGER,
        vendor_id      INTEGER NOT NULL,
        account_number TEXT    NOT NULL UNIQUE,
        meter_number   TEXT,
        rate_code      TEXT,
        FOREIGN KEY(property_id) REFERENCES Properties(property_id),
        FOREIGN KEY(vendor_id)   REFERENCES Vendors(vendor_id)
    )''')

    cursor.execute('''CREATE TABLE Bills (
        bill_id              INTEGER  PRIMARY KEY AUTOINCREMENT,
        account_id           INTEGER  NOT NULL,
        utility_type         TEXT     NOT NULL CHECK(utility_type IN ('Electric', 'Gas', 'Unknown')),
        billing_date         TEXT,
        service_period_start TEXT,
        service_period_end   TEXT,
        due_date             TEXT,
        total_amount         REAL     NOT NULL DEFAULT 0.0,
        usage_volume         REAL     DEFAULT 0.0,
        usage_unit           TEXT     CHECK(usage_unit IN ('kWh', 'Therms', 'Unknown')),
        demand_read          REAL     DEFAULT 0.0,
        demand_unit          TEXT,
        rate_code            TEXT,
        tariff_code          TEXT,
        is_anomaly_detected  INTEGER  NOT NULL DEFAULT 0,
        anomaly_reason       TEXT,
        anomaly_status       TEXT     NOT NULL DEFAULT 'Unreviewed'
                                      CHECK(anomaly_status IN ('Unreviewed', 'Confirmed', 'Dismissed', 'Claimed')),
        audit_timestamp      DATETIME NOT NULL,
        source_file          TEXT,
        FOREIGN KEY(account_id) REFERENCES Accounts(account_id)
    )''')

    cursor.execute('''CREATE TABLE Line_Items (
        item_id      INTEGER PRIMARY KEY AUTOINCREMENT,
        bill_id      INTEGER NOT NULL,
        category     TEXT    NOT NULL
                      CHECK(category IN (
                          'Fixed Monthly Charge',
                          'Delivery Charge',
                          'Supply Charge',
                          'Demand Charge',
                          'Taxes and Surcharges',
                          'Rider',
                          'Adjustment',
                          'Credit',
                          'Other'
                      )),
        description  TEXT,
        total_price  REAL    NOT NULL DEFAULT 0.0,
        FOREIGN KEY(bill_id) REFERENCES Bills(bill_id)
    )''')

    cursor.execute('''CREATE TABLE Audit_Claims (
        claim_id         INTEGER PRIMARY KEY AUTOINCREMENT,
        bill_id          INTEGER NOT NULL,
        claim_date       TEXT,
        claim_reason     TEXT,
        amount_disputed  REAL    DEFAULT 0.0,
        amount_recovered REAL    DEFAULT 0.0,
        status           TEXT    NOT NULL DEFAULT 'Open'
                          CHECK(status IN ('Open', 'Pending', 'Won', 'Lost', 'Partial')),
        vendor_response  TEXT,
        resolved_date    TEXT,
        FOREIGN KEY(bill_id) REFERENCES Bills(bill_id)
    )''')

    # Indexes
    cursor.execute('CREATE INDEX idx_clients_name         ON Clients(client_name)')
    cursor.execute('CREATE INDEX idx_properties_client    ON Properties(client_id)')
    cursor.execute('CREATE INDEX idx_accounts_number      ON Accounts(account_number)')
    cursor.execute('CREATE INDEX idx_accounts_property    ON Accounts(property_id)')
    cursor.execute('CREATE INDEX idx_accounts_vendor      ON Accounts(vendor_id)')
    cursor.execute('CREATE INDEX idx_bills_account        ON Bills(account_id)')
    cursor.execute('CREATE INDEX idx_bills_date           ON Bills(billing_date)')
    cursor.execute('CREATE INDEX idx_bills_utility        ON Bills(utility_type)')
    cursor.execute('CREATE INDEX idx_bills_anomaly        ON Bills(is_anomaly_detected)')
    cursor.execute('CREATE INDEX idx_bills_status         ON Bills(anomaly_status)')
    cursor.execute('CREATE INDEX idx_lineitems_bill       ON Line_Items(bill_id)')
    cursor.execute('CREATE INDEX idx_claims_bill          ON Audit_Claims(bill_id)')
    cursor.execute('CREATE INDEX idx_claims_status        ON Audit_Claims(status)')

if __name__ == "__main__":
    print("\n[SYSTEM] Purging old database and rebuilding...")
    force_delete_locked_db()

    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    cursor = conn.cursor()

    build_tables(cursor)
    conn.commit()
    conn.close()

    print("[SUCCESS] Industry-grade schema built successfully.")
    print(f"[INFO] Tables created: Clients, Properties, Vendors, Accounts, Bills, Line_Items, Audit_Claims")