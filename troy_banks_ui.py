"""
troy_banks_ui.py — Troy & Banks Audit Intelligence Platform
Connects directly to the existing pipeline, database, and anomaly detector.

Run with:
    streamlit run troy_banks_ui.py
"""

import io
import os
import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Troy & Banks Audit Intelligence",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── DB PATH ───────────────────────────────────────────────────────────────────
DB_PATH = os.path.join("/mnt/d", "Troy and banks", "troy_banks_relational.db")

# ── STYLING ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #1B2E4B 0%, #2563EB 100%);
        padding: 2rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        color: white;
    }
    .metric-card {
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 1.2rem;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .finding-urgent {
        background: #FEE2E2;
        border-left: 5px solid #DC2626;
        padding: 1rem;
        border-radius: 0 8px 8px 0;
        margin: 0.5rem 0;
    }
    .finding-high {
        background: #FEF3C7;
        border-left: 5px solid #D97706;
        padding: 1rem;
        border-radius: 0 8px 8px 0;
        margin: 0.5rem 0;
    }
    .finding-medium {
        background: #FFF7ED;
        border-left: 5px solid #EA580C;
        padding: 1rem;
        border-radius: 0 8px 8px 0;
        margin: 0.5rem 0;
    }
    .finding-low {
        background: #F0FDF4;
        border-left: 5px solid #16A34A;
        padding: 1rem;
        border-radius: 0 8px 8px 0;
        margin: 0.5rem 0;
    }
    .success-box {
        background: #D1FAE5;
        border: 1px solid #10B981;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    .error-box {
        background: #FEE2E2;
        border: 1px solid #EF4444;
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
    }
    div[data-testid="stTabs"] button {
        font-size: 1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

# ── HEADER ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="main-header">
    <h1 style="margin:0; font-size:2rem;">🏦 Troy & Banks</h1>
    <p style="margin:0.5rem 0 0 0; font-size:1.1rem; opacity:0.9;">
        Forensic Utility Audit Intelligence Platform
    </p>
    <p style="margin:0.3rem 0 0 0; font-size:0.85rem; opacity:0.7;">
        Automated Bill Ingestion · ML Anomaly Detection · Weather Normalization
    </p>
</div>
""", unsafe_allow_html=True)

# ── DB HELPER ─────────────────────────────────────────────────────────────────
def get_conn():
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA busy_timeout = 30000")
        return conn
    except Exception as e:
        st.error(f"Cannot connect to database at {DB_PATH}: {e}")
        raise


# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
tab1, tab2, tab3 = st.tabs([
    "📤  Upload Bills",
    "🚨  Anomaly Dashboard",
    "📊  Analytics & Recovery"
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — UPLOAD BILLS
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Upload Utility Bills")
    st.caption(
        "Upload one or more PDF bills. The system extracts all fields using "
        "local AI, validates the math, and stores the data in the database."
    )

    col_info1, col_info2, col_info3 = st.columns(3)
    with col_info1:
        st.info("**Step 1** — Upload your PDF bill")
    with col_info2:
        st.info("**Step 2** — AI extracts all fields locally")
    with col_info3:
        st.info("**Step 3** — Math is validated and data is stored")

    st.markdown("---")

    uploaded_files = st.file_uploader(
        "Drop utility bills here",
        type=["pdf"],
        accept_multiple_files=True,
        help="Supports digital PDFs and scanned documents"
    )

    if "upload_results" not in st.session_state:
        st.session_state.upload_results = []
    if "extractions" not in st.session_state:
        st.session_state.extractions = []

    col_b1, col_b2, _ = st.columns([1, 1, 4])
    with col_b1:
        process_clicked = st.button(
            "🚀 Process Bills",
            type="primary",
            disabled=not uploaded_files,
            use_container_width=True,
        )
    with col_b2:
        clear_clicked = st.button(
            "🗑️ Clear",
            disabled=not st.session_state.upload_results,
            use_container_width=True,
        )

    if clear_clicked:
        st.session_state.upload_results = []
        st.rerun()

    if process_clicked and uploaded_files:
        project_path = "/mnt/d/Troy and banks"
        if project_path not in sys.path:
            sys.path.insert(0, project_path)

        extractions = []
        progress = st.progress(0.0, text="Starting extraction...")

        for i, uploaded_file in enumerate(uploaded_files, start=1):
            progress.progress(
                (i - 1) / len(uploaded_files),
                text=f"Extracting {uploaded_file.name} ({i}/{len(uploaded_files)})..."
            )

            file_bytes = uploaded_file.getvalue()
            suffix = Path(uploaded_file.name).suffix or ".pdf"

            with tempfile.NamedTemporaryFile(
                delete=False,
                suffix=suffix,
                dir=project_path
            ) as tmp:
                tmp.write(file_bytes)
                tmp_path = tmp.name

            try:
                from extractor import process_utility_bill
                from llm_parser import (
                    parse_bill_to_json,
                    normalize_parsed_data,
                    preprocess_ocr_text
                )

                # Step 1 — Extract text
                raw_text = process_utility_bill(tmp_path)

                # Step 2 — Preprocess
                clean_text = preprocess_ocr_text(raw_text)

                # Step 3 — AI extraction (no DB insertion yet)
                json_str = parse_bill_to_json(
                    clean_text,
                    source_file=uploaded_file.name
                )
                parsed = json.loads(json_str)

                # Step 4 — Normalize
                normalized = normalize_parsed_data(parsed)
                normalized["source_file"] = uploaded_file.name

                extractions.append({
                    "file":       uploaded_file.name,
                    "status":     "EXTRACTED",
                    "data":       normalized,
                    "saved":      False,
                    "_bytes":     file_bytes,
                    "_tmp_path":  tmp_path,
                })

            except Exception as e:
                extractions.append({
                    "file":      uploaded_file.name,
                    "status":    "ERROR",
                    "data":      {},
                    "saved":     False,
                    "error":     str(e),
                    "_bytes":    file_bytes,
                    "_tmp_path": tmp_path,
                })

        progress.progress(1.0, text="Extraction complete.")
        progress.empty()
        st.session_state.extractions = extractions
        st.rerun()

    # ── REVIEW AND APPROVE ────────────────────────────────────────────────
    if "extractions" in st.session_state and st.session_state.extractions:
        extractions = st.session_state.extractions

        st.markdown("---")
        st.subheader("📋 Review Extracted Fields")
        st.caption(
            "Review what the AI extracted from each bill. "
            "Edit any field that is wrong or missing. "
            "Only approved bills will be saved to the database."
        )

        project_path = "/mnt/d/Troy and banks"
        if project_path not in sys.path:
            sys.path.insert(0, project_path)

        for idx, extraction in enumerate(extractions):
            fname = extraction["file"]
            status = extraction["status"]

            if status == "ERROR":
                st.error(f"❌ {fname} — Extraction failed: {extraction.get('error')}")
                continue

            data = extraction["data"]

            with st.expander(
                f"📄 {fname} — "
                f"{'✅ Saved' if extraction['saved'] else '⏳ Pending approval'}",
                expanded=not extraction["saved"]
            ):
                if extraction["saved"]:
                    st.success("This bill has been saved to the database.")
                    continue

                st.markdown("**Review and edit the extracted fields below:**")
                st.caption(
                    "🟢 Fields with values were extracted by AI. "
                    "🔴 Empty fields were not detected — fill them in if you know the values."
                )

                # ── EDITABLE FORM ─────────────────────────────────────────
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("**Bill Identity**")
                    provider = st.text_input(
                        "Provider Name",
                        value=data.get("provider_name") or "",
                        key=f"provider_{idx}",
                        help="e.g. ConEdison, National Grid, KeySpan"
                    )
                    account = st.text_input(
                        "Account Number",
                        value=str(data.get("account_number") or ""),
                        key=f"account_{idx}",
                    )
                    utility = st.selectbox(
                        "Utility Type",
                        ["Electric", "Gas", "Unknown"],
                        index=["Electric", "Gas", "Unknown"].index(
                            data.get("utility_type", "Unknown")
                            if data.get("utility_type") in ["Electric","Gas"]
                            else "Unknown"
                        ),
                        key=f"utility_{idx}",
                    )
                    billing_date = st.text_input(
                        "Billing Date (YYYY-MM-DD)",
                        value=data.get("billing_date") or "",
                        key=f"billing_date_{idx}",
                        help="Format: 2025-01-08"
                    )
                    due_date = st.text_input(
                        "Due Date (YYYY-MM-DD)",
                        value=data.get("due_date") or "",
                        key=f"due_date_{idx}",
                    )

                with col2:
                    st.markdown("**Financial Fields**")
                    total = st.number_input(
                        "Total Amount Due ($)",
                        value=float(data.get("total_amount_due") or 0.0),
                        min_value=0.0,
                        format="%.2f",
                        key=f"total_{idx}",
                    )
                    delivery = st.number_input(
                        "Delivery Charge ($)",
                        value=float(data.get("delivery_charge") or 0.0),
                        min_value=0.0,
                        format="%.2f",
                        key=f"delivery_{idx}",
                    )
                    supply = st.number_input(
                        "Supply Charge ($)",
                        value=float(data.get("supply_charge") or 0.0),
                        min_value=0.0,
                        format="%.2f",
                        key=f"supply_{idx}",
                    )
                    demand = st.number_input(
                        "Demand Charge ($)",
                        value=float(data.get("demand_charge") or 0.0),
                        min_value=0.0,
                        format="%.2f",
                        key=f"demand_{idx}",
                    )
                    taxes = st.number_input(
                        "Taxes & Surcharges ($)",
                        value=float(data.get("taxes_surcharges") or 0.0),
                        min_value=0.0,
                        format="%.2f",
                        key=f"taxes_{idx}",
                    )
                    credits = st.number_input(
                        "Credits / Balance Forward ($)",
                        value=float(data.get("credits") or 0.0),
                        min_value=0.0,
                        format="%.2f",
                        key=f"credits_{idx}",
                        help="Any credits or previous balance applied to this bill"
                    )

                col3, col4 = st.columns(2)
                with col3:
                    st.markdown("**Usage**")
                    usage = st.number_input(
                        "Usage Volume",
                        value=float(data.get("usage_volume") or 0.0),
                        min_value=0.0,
                        format="%.2f",
                        key=f"usage_{idx}",
                    )
                    unit = st.selectbox(
                        "Usage Unit",
                        ["kWh", "Therms", "CCF", "Unknown"],
                        index=["kWh","Therms","CCF","Unknown"].index(
                            data.get("usage_unit","Unknown")
                            if data.get("usage_unit") in ["kWh","Therms","CCF"]
                            else "Unknown"
                        ),
                        key=f"unit_{idx}",
                    )

                with col4:
                    st.markdown("**Rate Info**")
                    rate_code = st.text_input(
                        "Rate Code",
                        value=data.get("rate_code") or "",
                        key=f"rate_{idx}",
                    )
                    meter = st.text_input(
                        "Meter Number",
                        value=data.get("meter_number") or "",
                        key=f"meter_{idx}",
                    )

                # ── MATH CHECK PREVIEW ────────────────────────────────────
                st.markdown("---")
                st.markdown("**Math Validation Preview**")

                sub_total = delivery + supply + demand + taxes - credits
                diff = abs(sub_total - total)

                if total > 0 and sub_total > 0:
                    if diff > 0.02:
                        st.error(
                            f"⚠️ Math Error Detected: Sub-charges sum to "
                            f"${sub_total:,.2f} but total is ${total:,.2f}. "
                            f"Discrepancy: ${diff:,.2f}"
                        )
                    else:
                        st.success(
                            f"✅ Math Validated: All charges sum correctly "
                            f"to ${total:,.2f}"
                        )
                else:
                    st.info("Fill in the charge fields above to validate math.")

                # ── MISSING FIELDS WARNING ────────────────────────────────
                missing = []
                if not provider:
                    missing.append("Provider Name")
                if not account:
                    missing.append("Account Number")
                if not billing_date:
                    missing.append("Billing Date")
                if total == 0:
                    missing.append("Total Amount")

                if missing:
                    st.warning(
                        f"⚠️ Missing required fields: "
                        f"{', '.join(missing)}. "
                        f"Please fill these in before saving."
                    )

                # ── APPROVE BUTTON ────────────────────────────────────────
                st.markdown("---")
                col_save, col_skip = st.columns([2, 1])

                with col_save:
                    save_disabled = bool(missing)
                    if st.button(
                        "✅ Approve and Save to Database",
                        key=f"approve_{idx}",
                        type="primary",
                        disabled=save_disabled,
                        use_container_width=True,
                    ):
                        try:
                            from db_manager import insert_bill

                            # Build the approved data dict
                            approved = {
                                "provider_name":    provider,
                                "account_number":   account,
                                "utility_type":     utility,
                                "billing_date":     billing_date,
                                "due_date":         due_date,
                                "total_amount_due": total,
                                "delivery_charge":  delivery,
                                "supply_charge":    supply,
                                "demand_charge":    demand,
                                "taxes_surcharges": taxes,
                                "usage_volume":     usage,
                                "usage_unit":       unit,
                                "rate_code":        rate_code,
                                "meter_number":     meter,
                                "source_file":      fname,
                                "credits":          credits,
                                "fixed_monthly_charge": 0.0,
                                "service_period_start": "",
                                "service_period_end":   "",
                                "is_anomaly_detected":  diff > 0.02,
                                "anomaly_reason": (
                                    f"MATH AUDIT FAILED: Sub-charges sum to "
                                    f"${sub_total:.2f} but total due is "
                                    f"${total:.2f}. Discrepancy of "
                                    f"${diff:.2f}."
                                ) if diff > 0.02 else "",
                                "anomaly_status": "Unreviewed",
                            }

                            bill_id = insert_bill(approved)

                            if bill_id:
                                extraction["saved"] = True
                                extraction["bill_id"] = bill_id
                                st.session_state.extractions = extractions
                                st.success(
                                    f"✅ Bill saved to database "
                                    f"(Bill ID: {bill_id}). "
                                    f"Run anomaly detection to check "
                                    f"for pattern anomalies."
                                )
                                st.rerun()
                            else:
                                st.error(
                                    "Save failed — check that account "
                                    "number and provider are correct."
                                )
                        except Exception as e:
                            st.error(f"Save error: {e}")

                with col_skip:
                    if st.button(
                        "⏭️ Skip this bill",
                        key=f"skip_{idx}",
                        use_container_width=True,
                    ):
                        extraction["saved"] = True
                        extraction["skipped"] = True
                        st.session_state.extractions = extractions
                        st.rerun()

    # ── SHOW RESULTS ──────────────────────────────────────────────────────────
    if st.session_state.upload_results:
        results = st.session_state.upload_results
        success_count  = sum(1 for r in results if r["status"] == "SUCCESS")
        rejected_count = sum(1 for r in results if r["status"] in ("REJECTED","FAILED","ERROR"))

        m1, m2, m3 = st.columns(3)
        m1.metric("Bills Processed", len(results))
        m2.metric("Successfully Ingested", success_count)
        m3.metric("Failed / Rejected", rejected_count)

        st.markdown("---")

        for r in results:
            if r["status"] == "SUCCESS":
                anomaly_badge = (
                    "🔴 **Math Error Detected**" if r["anomaly"]
                    else "✅ Math Validated"
                )
                st.markdown(f"""
<div class="{'finding-urgent' if r['anomaly'] else 'success-box'}">
<strong>✅ {r['file']}</strong> — Bill #{r['bill_id']} stored successfully<br>
<strong>Client:</strong> {r['client']} &nbsp;|&nbsp;
<strong>Vendor:</strong> {r['vendor']} &nbsp;|&nbsp;
<strong>Account:</strong> {r['account']}<br>
<strong>Type:</strong> {r['utility']} &nbsp;|&nbsp;
<strong>Date:</strong> {r['date']} &nbsp;|&nbsp;
<strong>Total:</strong> ${r['total']:,.2f}<br>
<strong>Math Check:</strong> {anomaly_badge}
</div>
""", unsafe_allow_html=True)

                if r["anomaly"] and r["anomaly_reason"]:
                    st.warning(f"⚠️ {r['anomaly_reason']}")

                with st.expander(f"📋 Full breakdown — {r['file']}"):
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("**Charge Breakdown**")
                        breakdown = pd.DataFrame({
                            "Category": [
                                "Delivery Charge",
                                "Supply Charge",
                                "Demand Charge",
                                "Taxes & Surcharges",
                                "**Total**"
                            ],
                            "Amount": [
                                f"${r['delivery']:,.2f}",
                                f"${r['supply']:,.2f}",
                                f"${r['demand']:,.2f}",
                                f"${r['taxes']:,.2f}",
                                f"**${r['total']:,.2f}**"
                            ]
                        })
                        st.table(breakdown)
                    with col2:
                        st.markdown("**Bill Details**")
                        details = pd.DataFrame({
                            "Field": [
                                "Bill ID", "Client", "Property",
                                "Vendor", "Account", "Utility Type",
                                "Billing Date", "Usage"
                            ],
                            "Value": [
                                str(r["bill_id"]),
                                r["client"],
                                r.get("property", "—"),
                                r["vendor"],
                                r["account"],
                                r["utility"],
                                r["date"],
                                r["usage"]
                            ]
                        })
                        st.table(details)

            else:
                st.markdown(f"""
<div class="error-box">
<strong>❌ {r['file']}</strong> — {r['status']}<br>
{r.get('error', 'Unknown error')}
</div>
""", unsafe_allow_html=True)



#========Anomoly detection button===========
st.markdown("---")
st.markdown("**Run Anomaly Detection**")
st.caption(
    "After uploading bills, click below to run the full "
    "three-layer anomaly detection across all accounts."
)

if st.button(
    "🔍 Run Anomaly Detection on All Bills",
    type="primary",
    use_container_width=False
):
    with st.spinner(
        "Running detection... fetching weather data and "
        "training model. This takes 2-3 minutes."
    ):
        try:
            import subprocess
            result = subprocess.run(
                ["python", "anomaly_detector.py"],
                cwd="/mnt/d/Troy and banks",
                capture_output=True,
                text=True,
                timeout=300
            )
            if result.returncode == 0:
                st.success(
                    "Anomaly detection complete. "
                    "Check the Anomaly Dashboard tab for new findings."
                )
            else:
                st.error(f"Detection failed: {result.stderr}")
        except Exception as e:
            st.error(f"Error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — ANOMALY DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Anomaly Dashboard")
    st.caption(
        "Findings from three independent detection layers — "
        "Math Audit, Random Forest ML, and Weather Normalization. "
        "All reasons are in plain English. No technical jargon."
    )

    # ── SUMMARY METRICS ───────────────────────────────────────────────────────
    try:
        conn = get_conn()
        summary = conn.execute("""
            SELECT
                COUNT(*) as total,
                ROUND(SUM(potential_recovery), 2) as total_recovery,
                SUM(CASE WHEN severity='URGENT' THEN 1 ELSE 0 END) as urgent,
                SUM(CASE WHEN severity='HIGH'   THEN 1 ELSE 0 END) as high,
                SUM(CASE WHEN severity='MEDIUM' THEN 1 ELSE 0 END) as medium,
                SUM(CASE WHEN severity='LOW'    THEN 1 ELSE 0 END) as low,
                SUM(CASE WHEN reviewed=0        THEN 1 ELSE 0 END) as unreviewed
            FROM Anomaly_Analysis
        """).fetchone()
        conn.close()

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total Findings",     summary["total"])
        m2.metric("Potential Recovery", f"${summary['total_recovery']:,.0f}")
        m3.metric("🔴 Urgent",          summary["urgent"])
        m4.metric("🟡 High Priority",   summary["high"])
        m5.metric("Unreviewed",         summary["unreviewed"])

    except Exception as e:
        st.error(f"Database error: {e}")

    st.markdown("---")

    # ── FILTERS ───────────────────────────────────────────────────────────────
    col_f1, col_f2, col_f3 = st.columns(3)

    with col_f1:
        try:
            conn = get_conn()
            clients = ["All Clients"] + [
                r[0] for r in conn.execute(
                    "SELECT DISTINCT client_name FROM Clients ORDER BY client_name"
                ).fetchall()
            ]
            conn.close()
        except:
            clients = ["All Clients"]
        selected_client = st.selectbox("Filter by Client", clients)

    with col_f2:
        selected_severity = st.selectbox(
            "Filter by Severity",
            ["All", "URGENT", "HIGH", "MEDIUM", "LOW"]
        )

    with col_f3:
        show_reviewed = st.checkbox("Show reviewed findings", value=False)

    # ── LOAD FINDINGS ─────────────────────────────────────────────────────────
    try:
        conn = get_conn()

        where_clauses = []
        params = []

        if selected_client != "All Clients":
            where_clauses.append("c.client_name = ?")
            params.append(selected_client)

        if selected_severity != "All":
            where_clauses.append("aa.severity = ?")
            params.append(selected_severity)

        if not show_reviewed:
            where_clauses.append("aa.reviewed = 0")

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        findings = conn.execute(f"""
            SELECT
                aa.analysis_id,
                aa.severity,
                aa.detection_layer,
                aa.plain_english_reason,
                aa.potential_recovery,
                aa.reviewed,
                b.utility_type,
                b.billing_date,
                b.total_amount,
                b.bill_id,
                p.property_name,
                v.vendor_name,
                c.client_name
            FROM Anomaly_Analysis aa
            JOIN Bills b ON aa.bill_id = b.bill_id
            JOIN Accounts a ON b.account_id = a.account_id
            JOIN Properties p ON a.property_id = p.property_id
            JOIN Clients c ON p.client_id = c.client_id
            JOIN Vendors v ON a.vendor_id = v.vendor_id
            {where_sql}
            ORDER BY
                CASE aa.severity
                    WHEN 'URGENT' THEN 1
                    WHEN 'HIGH'   THEN 2
                    WHEN 'MEDIUM' THEN 3
                    WHEN 'LOW'    THEN 4
                END,
                aa.potential_recovery DESC
        """, params).fetchall()
        conn.close()

    except Exception as e:
        st.error(f"Error loading findings: {e}")
        findings = []

    if not findings:
        st.info("No findings match your filters.")
    else:
        # Group by client
        from itertools import groupby

        findings_list = [dict(f) for f in findings]
        clients_seen = []
        by_client = {}
        for f in findings_list:
            cn = f["client_name"]
            if cn not in by_client:
                by_client[cn] = []
                clients_seen.append(cn)
            by_client[cn].append(f)

        severity_icons = {
            "URGENT": "🔴",
            "HIGH":   "🟡",
            "MEDIUM": "🟠",
            "LOW":    "🟢"
        }
        layer_labels = {
            "MATH":     "Math Audit",
            "ML":       "Pattern Analysis",
            "COMBINED": "Pattern + Weather"
        }

        for client_name in clients_seen:
            client_findings = by_client[client_name]
            client_recovery = sum(f["potential_recovery"] for f in client_findings)

            with st.expander(
                f"**{client_name}** — "
                f"{len(client_findings)} findings — "
                f"Potential recovery: ${client_recovery:,.0f}",
                expanded=(selected_client != "All Clients")
            ):
                current_severity = None
                for f in client_findings:
                    if f["severity"] != current_severity:
                        current_severity = f["severity"]
                        icon = severity_icons.get(current_severity, "")
                        st.markdown(
                            f"### {icon} {current_severity.title()} Priority"
                        )

                    layer = layer_labels.get(f["detection_layer"],
                                            f["detection_layer"])
                    reviewed_badge = " ✓ Reviewed" if f["reviewed"] else ""
                    date_str = f["billing_date"][:7] if f["billing_date"] else "?"

                    css_class = {
                        "URGENT": "finding-urgent",
                        "HIGH":   "finding-high",
                        "MEDIUM": "finding-medium",
                        "LOW":    "finding-low"
                    }.get(f["severity"], "finding-low")

                    st.markdown(f"""
<div class="{css_class}">
<strong>Finding #{f['analysis_id']}{reviewed_badge}</strong> &nbsp;|&nbsp;
Detected by: <em>{layer}</em><br>
<strong>{f['property_name']}</strong> —
{f['utility_type']} —
{f['vendor_name']} —
{date_str}<br>
Bill total: <strong>${f['total_amount']:,.0f}</strong> &nbsp;|&nbsp;
Potential overcharge: <strong>${f['potential_recovery']:,.0f}</strong><br>
<br>
{f['plain_english_reason']}
</div>
""", unsafe_allow_html=True)

                    if not f["reviewed"]:
                        col_c, col_d, _ = st.columns([1, 1, 4])
                        with col_c:
                            if st.button(
                                "✅ Confirm",
                                key=f"confirm_{f['analysis_id']}",
                                use_container_width=True
                            ):
                                try:
                                    conn = get_conn()
                                    conn.execute(
                                        "UPDATE Anomaly_Analysis "
                                        "SET reviewed=1 WHERE analysis_id=?",
                                        (f["analysis_id"],)
                                    )
                                    conn.execute(
                                        "UPDATE Bills SET anomaly_status='Confirmed' "
                                        "WHERE bill_id=?",
                                        (f["bill_id"],)
                                    )
                                    conn.execute("""
                                        INSERT INTO Audit_Claims
                                        (bill_id, claim_date, claim_reason,
                                         amount_disputed, status)
                                        VALUES (?, ?, ?, ?, 'Open')
                                    """, (
                                        f["bill_id"],
                                        datetime.now().strftime("%Y-%m-%d"),
                                        f["plain_english_reason"],
                                        f["potential_recovery"]
                                    ))
                                    conn.commit()
                                    conn.close()
                                    st.success(
                                        f"Finding #{f['analysis_id']} confirmed. "
                                        f"Audit claim created for "
                                        f"${f['potential_recovery']:,.2f}."
                                    )
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")

                        with col_d:
                            if st.button(
                                "❌ Dismiss",
                                key=f"dismiss_{f['analysis_id']}",
                                use_container_width=True
                            ):
                                try:
                                    conn = get_conn()
                                    conn.execute(
                                        "UPDATE Anomaly_Analysis "
                                        "SET reviewed=1 WHERE analysis_id=?",
                                        (f["analysis_id"],)
                                    )
                                    conn.execute(
                                        "UPDATE Bills SET anomaly_status='Dismissed' "
                                        "WHERE bill_id=?",
                                        (f["bill_id"],)
                                    )
                                    conn.commit()
                                    conn.close()
                                    st.info(
                                        f"Finding #{f['analysis_id']} dismissed."
                                    )
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")

                    st.markdown("")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Analytics & Recovery Summary")
    st.caption(
        "Business intelligence derived from the anomaly detection system. "
        "Shows where the money is and what types of errors are most common."
    )

    try:
        conn = get_conn()

        # Recovery by client
        by_client = pd.read_sql_query("""
            SELECT c.client_name,
                   COUNT(aa.analysis_id) as findings,
                   ROUND(SUM(aa.potential_recovery), 2) as recovery,
                   SUM(CASE WHEN aa.severity='URGENT' THEN 1 ELSE 0 END) as urgent
            FROM Anomaly_Analysis aa
            JOIN Bills b ON aa.bill_id = b.bill_id
            JOIN Accounts a ON b.account_id = a.account_id
            JOIN Properties p ON a.property_id = p.property_id
            JOIN Clients c ON p.client_id = c.client_id
            GROUP BY c.client_name
            ORDER BY recovery DESC
        """, conn)

        # By detection layer
        by_layer = pd.read_sql_query("""
            SELECT detection_layer,
                   COUNT(*) as findings,
                   ROUND(SUM(potential_recovery), 2) as recovery
            FROM Anomaly_Analysis
            GROUP BY detection_layer
            ORDER BY recovery DESC
        """, conn)

        # By utility type
        by_utility = pd.read_sql_query("""
            SELECT b.utility_type,
                   COUNT(aa.analysis_id) as findings,
                   ROUND(SUM(aa.potential_recovery), 2) as recovery
            FROM Anomaly_Analysis aa
            JOIN Bills b ON aa.bill_id = b.bill_id
            GROUP BY b.utility_type
        """, conn)

        # Monthly bill trend
        monthly = pd.read_sql_query("""
            SELECT strftime('%Y-%m', billing_date) as month,
                   COUNT(*) as bills,
                   ROUND(SUM(total_amount), 2) as total_spend
            FROM Bills
            WHERE billing_date IS NOT NULL
            GROUP BY month
            ORDER BY month
        """, conn)

        # Top findings
        top_findings = pd.read_sql_query("""
            SELECT aa.analysis_id,
                   aa.severity,
                   aa.detection_layer,
                   ROUND(aa.potential_recovery, 2) as potential_recovery,
                   b.utility_type,
                   b.billing_date,
                   b.total_amount,
                   c.client_name,
                   p.property_name,
                   v.vendor_name
            FROM Anomaly_Analysis aa
            JOIN Bills b ON aa.bill_id = b.bill_id
            JOIN Accounts a ON b.account_id = a.account_id
            JOIN Properties p ON a.property_id = p.property_id
            JOIN Clients c ON p.client_id = c.client_id
            JOIN Vendors v ON a.vendor_id = v.vendor_id
            WHERE aa.reviewed = 0
            ORDER BY aa.potential_recovery DESC
            LIMIT 10
        """, conn)

        conn.close()

        # ── CHARTS ROW 1 ──────────────────────────────────────────────────────
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("**Potential Recovery by Client**")
            if not by_client.empty:
                st.bar_chart(
                    by_client.set_index("client_name")["recovery"],
                    use_container_width=True,
                    color="#2563EB"
                )

        with col_b:
            st.markdown("**Findings by Detection Method**")
            if not by_layer.empty:
                layer_display = by_layer.copy()
                layer_display["detection_layer"] = layer_display[
                    "detection_layer"
                ].map({
                    "MATH":     "Math Audit",
                    "ML":       "Pattern Analysis",
                    "COMBINED": "Pattern + Weather"
                })
                st.bar_chart(
                    layer_display.set_index("detection_layer")["recovery"],
                    use_container_width=True,
                    color="#10B981"
                )

        # ── CHARTS ROW 2 ──────────────────────────────────────────────────────
        col_c, col_d = st.columns(2)

        with col_c:
            st.markdown("**Monthly Billing Volume**")
            if not monthly.empty:
                st.line_chart(
                    monthly.set_index("month")["total_spend"],
                    use_container_width=True,
                    color="#D97706"
                )

        with col_d:
            st.markdown("**Recovery by Utility Type**")
            if not by_utility.empty:
                st.bar_chart(
                    by_utility.set_index("utility_type")["recovery"],
                    use_container_width=True,
                    color="#7C3AED"
                )

        st.markdown("---")

        # ── SUMMARY TABLE ─────────────────────────────────────────────────────
        st.markdown("**Recovery Summary by Client**")
        display_client = by_client.copy()
        display_client["recovery"] = display_client["recovery"].apply(
            lambda x: f"${x:,.0f}"
        )
        display_client.columns = [
            "Client", "Total Findings", "Potential Recovery", "Urgent Findings"
        ]
        st.dataframe(
            display_client,
            use_container_width=True,
            hide_index=True
        )

        st.markdown("---")

        # ── TOP 10 FINDINGS ───────────────────────────────────────────────────
        st.markdown("**Top 10 Highest Value Findings**")
        if not top_findings.empty:
            display_top = top_findings[[
                "analysis_id", "client_name", "property_name",
                "utility_type", "billing_date", "potential_recovery",
                "severity", "detection_layer"
            ]].copy()
            display_top["potential_recovery"] = display_top[
                "potential_recovery"
            ].apply(lambda x: f"${x:,.0f}")
            display_top["detection_layer"] = display_top["detection_layer"].map({
                "MATH":     "Math Audit",
                "ML":       "Pattern Analysis",
                "COMBINED": "Pattern + Weather"
            })
            display_top.columns = [
                "ID", "Client", "Property", "Utility",
                "Bill Date", "Recovery", "Severity", "Detected By"
            ]
            st.dataframe(
                display_top,
                use_container_width=True,
                hide_index=True
            )

        # ── MODEL PERFORMANCE ─────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("**Random Forest Model Performance**")
        col_p1, col_p2, col_p3, col_p4 = st.columns(4)
        col_p1.metric("Catch Rate",       "98.4%",  "↑ vs 60% manual")
        col_p2.metric("False Alarm Rate", "0.3%",   "↓ minimal wasted time")
        col_p3.metric("ROC-AUC Score",    "0.9992", "Near perfect")
        col_p4.metric("Mean F1 Score",    "0.975",  "5-fold cross validation")

        st.caption(
            "Model trained on 2,016 synthetic commercial bills. "
            "98.4% of real anomalies detected. Only 0.3% of normal bills "
            "incorrectly flagged. Will be retrained on real Troy & Banks "
            "confirmed findings when available."
        )

    except Exception as e:
        st.error(f"Error loading analytics: {e}")
        st.exception(e)

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🏦 Troy & Banks")
    st.caption("Forensic Utility Audit Intelligence")
    st.markdown("---")

    # DB stats
    try:
        conn = get_conn()
        stats = conn.execute("""
            SELECT
                (SELECT COUNT(*) FROM Bills)    as bills,
                (SELECT COUNT(*) FROM Clients)  as clients,
                (SELECT COUNT(*) FROM Accounts) as accounts,
                (SELECT COUNT(*) FROM Anomaly_Analysis WHERE reviewed=0)
                    as unreviewed
        """).fetchone()
        conn.close()

        st.markdown("**Database**")
        st.metric("Total Bills",      stats["bills"])
        st.metric("Clients",          stats["clients"])
        st.metric("Accounts",         stats["accounts"])
        st.metric("Pending Review",   stats["unreviewed"])

    except:
        st.warning("Database not connected")

    st.markdown("---")
    st.markdown("**Detection Layers**")
    st.markdown("✅ Math Audit")
    st.markdown("✅ Random Forest ML")
    st.markdown("✅ Weather Normalization")
    st.markdown("---")
    st.markdown("**Stack**")
    st.caption("Python 3.12 · SQLite · Ollama")
    st.caption("qwen2.5-coder:7b · scikit-learn")
    st.caption("Open-Meteo API · Streamlit")
    st.markdown("---")
    st.caption("SUNY Buffalo · Team PM · 2025–2026")