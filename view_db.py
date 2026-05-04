import sqlite3

DB_NAME = "troy_banks_relational.db"

def show_vault():
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        
        print("\n=== 🏦 TROY & BANKS SECURE VAULT ===")
        
        print("\n--- 🏢 VENDORS & ACCOUNTS ---")
        cursor.execute("""
            SELECT Vendors.vendor_name, Accounts.account_number 
            FROM Accounts 
            JOIN Vendors ON Accounts.vendor_id = Vendors.vendor_id
        """)
        for row in cursor.fetchall():
            print(f"Vendor: {row} | Account: {row}")
            
        print("\n--- 🧾 INGESTED BILLS ---")
        cursor.execute("""
            SELECT utility_type, total_amount, is_anomaly_detected, anomaly_reason 
            FROM Bills
        """)
        for row in cursor.fetchall():
            anomaly_status = "🚨 YES" if row == 1 else "✅ NO"
            print(f"Type: {row} | Total: ${row:.2f} | Anomaly: {anomaly_status}")
            if row == 1:
                print(f"   ↳ Reason: {row}")
                
        conn.close()
        print("\n===================================\n")
        
    except Exception as e:
        print(f"Error reading database: {e}")

def run_sql():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM Bills")
    
    # Grab all the data
    rows = cursor.fetchall()
    
    conn.close()
    
    # Do something with it
    for row in rows:
        print(row)
        
    return rows
if __name__ == "__main__":
    run_sql()