import sqlite3

DB_NAME = "troy_banks_relational.db"

def build_database():
    print("=== BUILDING RELATIONAL DATABASE ===")
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA foreign_keys = ON;") 
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Vendors (vendor_id INTEGER PRIMARY KEY AUTOINCREMENT, vendor_name TEXT UNIQUE)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Accounts (account_id INTEGER PRIMARY KEY AUTOINCREMENT, vendor_id INTEGER, account_number TEXT UNIQUE, FOREIGN KEY(vendor_id) REFERENCES Vendors(vendor_id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Bills (bill_id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER, utility_type TEXT, billing_date TEXT, total_amount REAL, is_anomaly_detected INTEGER, anomaly_reason TEXT, audit_timestamp DATETIME, FOREIGN KEY(account_id) REFERENCES Accounts(account_id))''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS Line_Items (item_id INTEGER PRIMARY KEY AUTOINCREMENT, bill_id INTEGER, category TEXT, total_price REAL, FOREIGN KEY(bill_id) REFERENCES Bills(bill_id))''')
    
    conn.commit()
    conn.close()
    print(f"[SUCCESS] {DB_NAME} has been built and unlocked. Ready for ingestion.")

if __name__ == "__main__":
    build_database()