import sqlite3
import json
from mcp.server.fastmcp import FastMCP

DB_NAME = "troy_banks_relational.db"

mcp = FastMCP("Troy & Banks Audit Intelligence")


def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@mcp.tool()
def get_all_clients() -> str:
    """Get all clients in the Troy & Banks audit system."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT c.client_id, c.client_name, c.industry,
               COUNT(DISTINCT p.property_id) as properties,
               COUNT(DISTINCT a.account_id) as accounts
        FROM Clients c
        LEFT JOIN Properties p ON c.client_id = p.client_id
        LEFT JOIN Accounts a ON p.property_id = a.property_id
        GROUP BY c.client_id
        ORDER BY c.client_name
    ''')
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return json.dumps(rows, indent=2)


@mcp.tool()
def get_client_summary(client_name: str) -> str:
    """
    Get full billing summary for a specific client.
    Shows all properties, accounts, total spend, and anomaly count.
    """
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT
            c.client_name,
            c.industry,
            p.property_name,
            p.address,
            p.city,
            a.account_number,
            v.vendor_name,
            b.utility_type,
            COUNT(b.bill_id) as total_bills,
            ROUND(SUM(b.total_amount), 2) as total_spend,
            ROUND(AVG(b.total_amount), 2) as avg_bill,
            SUM(b.is_anomaly_detected) as anomalies
        FROM Clients c
        JOIN Properties p ON c.client_id = p.client_id
        JOIN Accounts a ON p.property_id = a.property_id
        JOIN Vendors v ON a.vendor_id = v.vendor_id
        JOIN Bills b ON a.account_id = b.account_id
        WHERE LOWER(c.client_name) LIKE LOWER(?)
        GROUP BY a.account_id, b.utility_type
        ORDER BY p.property_name, b.utility_type
    ''', (f'%{client_name}%',))
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    if not rows:
        return json.dumps({"message": f"No client found matching '{client_name}'"})
    return json.dumps(rows, indent=2)


@mcp.tool()
def get_anomalies(
    client_name: str = "",
    status: str = "",
    utility_type: str = "",
    limit: int = 20
) -> str:
    """
    Get anomalous bills. Filter by client name, status
    (Unreviewed/Confirmed/Dismissed/Claimed), or utility type (Electric/Gas).
    """
    conn = get_db()
    cursor = conn.cursor()

    query = '''
        SELECT
            c.client_name,
            p.property_name,
            v.vendor_name,
            a.account_number,
            b.utility_type,
            b.billing_date,
            b.total_amount,
            b.anomaly_reason,
            b.anomaly_status,
            b.bill_id
        FROM Bills b
        JOIN Accounts a ON b.account_id = a.account_id
        JOIN Properties p ON a.property_id = p.property_id
        JOIN Clients c ON p.client_id = c.client_id
        JOIN Vendors v ON a.vendor_id = v.vendor_id
        WHERE b.is_anomaly_detected = 1
    '''
    params = []

    if client_name:
        query += " AND LOWER(c.client_name) LIKE LOWER(?)"
        params.append(f'%{client_name}%')

    if status:
        query += " AND b.anomaly_status = ?"
        params.append(status)

    if utility_type:
        query += " AND b.utility_type = ?"
        params.append(utility_type)

    query += " ORDER BY b.billing_date DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    if not rows:
        return json.dumps({"message": "No anomalies found matching the filters."})
    return json.dumps(rows, indent=2)


@mcp.tool()
def get_bills(
    account_number: str = "",
    client_name: str = "",
    utility_type: str = "",
    start_date: str = "",
    end_date: str = "",
    limit: int = 24
) -> str:
    """
    Query bills with flexible filters.
    account_number: filter by specific account.
    client_name: filter by client.
    utility_type: Electric or Gas.
    start_date / end_date: date range in YYYY-MM-DD format.
    limit: max number of results (default 24).
    """
    conn = get_db()
    cursor = conn.cursor()

    query = '''
        SELECT
            c.client_name,
            p.property_name,
            v.vendor_name,
            a.account_number,
            b.utility_type,
            b.billing_date,
            b.service_period_start,
            b.service_period_end,
            b.total_amount,
            b.usage_volume,
            b.usage_unit,
            b.demand_read,
            b.is_anomaly_detected,
            b.anomaly_status,
            b.bill_id
        FROM Bills b
        JOIN Accounts a ON b.account_id = a.account_id
        JOIN Properties p ON a.property_id = p.property_id
        JOIN Clients c ON p.client_id = c.client_id
        JOIN Vendors v ON a.vendor_id = v.vendor_id
        WHERE 1=1
    '''
    params = []

    if account_number:
        query += " AND a.account_number = ?"
        params.append(account_number)

    if client_name:
        query += " AND LOWER(c.client_name) LIKE LOWER(?)"
        params.append(f'%{client_name}%')

    if utility_type:
        query += " AND b.utility_type = ?"
        params.append(utility_type)

    if start_date:
        query += " AND b.billing_date >= ?"
        params.append(start_date)

    if end_date:
        query += " AND b.billing_date <= ?"
        params.append(end_date)

    query += " ORDER BY b.billing_date DESC LIMIT ?"
    params.append(limit)

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()

    if not rows:
        return json.dumps({"message": "No bills found matching the filters."})
    return json.dumps(rows, indent=2)


@mcp.tool()
def compare_periods(
    account_number: str,
    period1_start: str,
    period1_end: str,
    period2_start: str,
    period2_end: str
) -> str:
    """
    Compare billing data between two time periods for an account.
    Dates in YYYY-MM-DD format.
    Example: compare Jan-Jun 2023 vs Jan-Jun 2024.
    """
    conn = get_db()
    cursor = conn.cursor()

    def get_period_stats(start, end):
        cursor.execute('''
            SELECT
                COUNT(*) as bill_count,
                ROUND(SUM(b.total_amount), 2) as total_spend,
                ROUND(AVG(b.total_amount), 2) as avg_bill,
                ROUND(SUM(b.usage_volume), 2) as total_usage,
                ROUND(AVG(b.usage_volume), 2) as avg_usage,
                SUM(b.is_anomaly_detected) as anomalies
            FROM Bills b
            JOIN Accounts a ON b.account_id = a.account_id
            WHERE a.account_number = ?
            AND b.billing_date BETWEEN ? AND ?
        ''', (account_number, start, end))
        return dict(cursor.fetchone())

    period1 = get_period_stats(period1_start, period1_end)
    period2 = get_period_stats(period2_start, period2_end)

    # Calculate changes
    spend_change = 0
    usage_change = 0
    if period1["total_spend"] and period2["total_spend"]:
        spend_change = round(
            ((period2["total_spend"] - period1["total_spend"]) / period1["total_spend"]) * 100, 1
        )
    if period1["total_usage"] and period2["total_usage"]:
        usage_change = round(
            ((period2["total_usage"] - period1["total_usage"]) / period1["total_usage"]) * 100, 1
        )

    result = {
        "account_number": account_number,
        "period_1": {
            "range": f"{period1_start} to {period1_end}",
            "stats": period1
        },
        "period_2": {
            "range": f"{period2_start} to {period2_end}",
            "stats": period2
        },
        "changes": {
            "spend_change_pct": spend_change,
            "usage_change_pct": usage_change,
            "spend_trend": "UP" if spend_change > 0 else "DOWN",
            "usage_trend": "UP" if usage_change > 0 else "DOWN"
        }
    }
    conn.close()
    return json.dumps(result, indent=2)


@mcp.tool()
def get_top_spenders(
    utility_type: str = "",
    year: int = 0,
    limit: int = 10
) -> str:
    """
    Get top spending accounts ranked by total spend.
    Filter by utility_type (Electric/Gas) and/or year.
    """
    conn = get_db()
    cursor = conn.cursor()

    query = '''
        SELECT
            c.client_name,
            p.property_name,
            v.vendor_name,
            a.account_number,
            b.utility_type,
            COUNT(b.bill_id) as bill_count,
            ROUND(SUM(b.total_amount), 2) as total_spend,
            ROUND(AVG(b.total_amount), 2) as avg_monthly,
            SUM(b.is_anomaly_detected) as anomalies
        FROM Bills b
        JOIN Accounts a ON b.account_id = a.account_id
        JOIN Properties p ON a.property_id = p.property_id
        JOIN Clients c ON p.client_id = c.client_id
        JOIN Vendors v ON a.vendor_id = v.vendor_id
        WHERE 1=1
    '''
    params = []

    if utility_type:
        query += " AND b.utility_type = ?"
        params.append(utility_type)

    if year:
        query += " AND strftime('%Y', b.billing_date) = ?"
        params.append(str(year))

    query += '''
        GROUP BY a.account_id, b.utility_type
        ORDER BY total_spend DESC
        LIMIT ?
    '''
    params.append(limit)

    cursor.execute(query, params)
    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return json.dumps(rows, indent=2)


@mcp.tool()
def run_sql(query: str) -> str:
    """
    Run a read-only SQL query against the database.
    Use this for complex custom queries.
    Only SELECT statements are allowed.
    Tables: Clients, Properties, Vendors, Accounts, Bills, Line_Items, Audit_Claims.
    """
    if not query.strip().upper().startswith("SELECT"):
        return json.dumps({"error": "Only SELECT queries are allowed."})

    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(query)
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return json.dumps(rows, indent=2)
    except Exception as e:
        return json.dumps({"error": str(e)})


if __name__ == "__main__":
    mcp.run(transport="stdio")