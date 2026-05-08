# Troy & Banks — Forensic Utility Audit Intelligence Platform

> Reducing 45 days of manual bill organization to minutes using AI, machine learning, and weather-normalized anomaly detection.

Built for **Troy & Banks** — a 33-year-old forensic energy auditing firm in Buffalo, NY that has reviewed 1.6 million utility bills and recovered over $1 billion in overcharges for commercial clients.

**Team PM · V Sai Mahesh · William N. · SUNY Buffalo · 2025–2026**

---

## The Problem

Troy & Banks audits commercial utility bills on a pure contingency model — they earn only when they find billing errors. Before any analysis could begin, auditors spent **45 days per client engagement** manually opening PDFs, reading numbers, and building spreadsheets by hand. That is 45 days of unpaid work on every single engagement.

---

## What We Built

A complete forensic audit intelligence platform that automates bill ingestion, validates data, detects anomalies using multiple methods, and surfaces findings in plain English to non-technical auditors.

```
PDF Bill → AI Extraction → Math Validation → Database → Anomaly Detection → Auditor Dashboard
```

Everything runs locally. No client data leaves the machine.

---

## Feature

1. Upload — drop in PDF, JPG, or PNG bills (single or batch)
2. Extract — pull structured fields using either Gemini Vision or local OCR + LLM
3. Review — verify extracted values against the original bill side-by-side, edit anything that's wrong
4. Save — commit corrected data to a SQLite database with auto-managed customer and provider records
5. Query — ask plain-English questions or run pre-built analytics over the bills database by using MCP Server
6. Anomalies Detection - three layers of detection to identify anomaly

## Extraction Result

From unstructure format like invoices -> structure tables

<img width="2216" height="1442" alt="image" src="https://github.com/user-attachments/assets/8fb08ac0-2a47-4d8c-b19b-d96de54216f5" />

---

## Audit Result

| Metric | Value |
|--------|-------|
| Bills analyzed | 2,016 across 5 clients, 6 years |
| Potential recovery identified | $541,937 |
| Anomaly findings | 304 |
| ML catch rate | 98.4% |
| False alarm rate | 0.3% |
| ROC-AUC Score | 0.9992 |
| Mean F1 (5-fold CV) | 0.975 |

---

## System Architecture

```
extractor.py        PDF ingestion — PyMuPDF for digital, Tesseract+OpenCV for scanned
                    Content-based page filter removes non-billing pages

llm_parser.py       AI normalization using Ollama (qwen2.5-coder:7b) locally
                    Python math audit independently validates all arithmetic

db_manager.py       Validation gate rejects bad extractions before DB insertion
                    8-table SQLite schema in 3NF with WAL mode for WSL stability

anomaly_detector.py Layer 1 — Math Audit: catches arithmetic errors (deterministic)
                    Layer 2 — Random Forest ML: catches multivariate pattern anomalies
                    Layer 3 — Weather Normalization: HDD/CDD context via Open-Meteo API

troy_banks_ui.py    Streamlit web interface
                    Tab 1: Upload bills — AI pre-fills, auditor reviews, approves to save
                    Tab 2: Anomaly dashboard — findings in plain English, confirm/dismiss
                    Tab 3: Analytics — recovery charts, detection breakdown, ML metrics

mcp_server.py       14-tool MCP server connected to Claude Desktop
                    Auditors query six years of billing history in plain English
```

---

## Anomaly Detection

The three layers are independent — each catches errors the others miss.

**Math Audit** catches calculation errors where sub-charges do not sum to the bill total. Pure Python arithmetic. 100% deterministic. Runs on every bill.

**Random Forest ML** is trained on 2,016 labeled bills (306 anomalous, 1,710 normal) using 12 relative features — Z-scores against monthly and overall baselines, effective delivery rate ratios, charge composition percentages, seasonality, and industry type. Relative features ensure the model generalizes to accounts of any dollar scale. Requires 12 months of account history before activating.

**Weather Normalization** fetches real historical temperature data from Open-Meteo API for the exact billing period. Uses Heating Degree Days for gas bills and Cooling Degree Days for electric bills with industry-specific sensitivity weights. Escalates findings where high usage cannot be explained by weather and clears false positives where weather fully accounts for a spike.

| Layer | Findings | Recovery |
|-------|----------|----------|
| Combined ML + Weather | 113 | $426,626 |
| Math Audit | 134 | $90,667 |
| ML Pattern Only | 57 | $24,644 |
| Total | 304 | $541,937 |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| PDF extraction | PyMuPDF, Tesseract, OpenCV |
| AI normalization | Ollama + qwen2.5-coder:7b (local) |
| Machine learning | scikit-learn RandomForestClassifier |
| Weather data | Open-Meteo historical archive API |
| Database | SQLite3 with WAL mode |
| Web interface | Streamlit |
| Natural language queries | Anthropic MCP + Claude Desktop |
| Runtime | Python 3.12, WSL2 Ubuntu 20.04 |

---

## Setup

Requires Windows 10/11 with WSL2, 8GB RAM minimum, and Ollama installed on Windows.

```bash
git clone https://github.com/SaiM1710/troy-banks-audit.git
cd troy-banks-audit

python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

sudo apt install -y tesseract-ocr poppler-utils

python schema.py
python seed_data.py
python anomaly_detector.py

python -m streamlit run troy_banks_ui.py
```

Python 3.12 must be built from source on Ubuntu 20.04. Full setup guide including Python build, Ollama model pull, and Claude Desktop MCP configuration is in the project book.

---

## MCP Tools (14 total)

| Tool | Description |
|------|-------------|
| get_anomaly_dashboard | All findings grouped by client and severity in plain English |
| get_recovery_summary | Total potential recovery by client and detection method |
| confirm_finding | Confirm a finding and automatically create an Audit_Claim |
| dismiss_finding | Dismiss a finding as not a real error |
| get_detection_insights | Error pattern analysis from feature importance |
| compare_periods | Period-over-period spend comparison for any account |
| run_sql | Read-only direct database queries |

Example Claude Desktop queries:

```
Show me the anomaly dashboard for Great Lakes Manufacturing
What is the total potential recovery across all clients?
What types of billing errors are we finding most often?
```

---

## Privacy

All processing is local. Ollama runs on localhost, SQLite is a local file, and the MCP server communicates over stdio. Open-Meteo receives only date ranges and GPS coordinates — never billing data. For production cloud deployment, a PII anonymization layer strips client identifiers before any external API call.

---

## Upgrade Path

| Component | Current | Production |
|-----------|---------|------------|
| OCR | Tesseract | Paid cloud OCR — one function change |
| LLM | Ollama 7B | Paid cloud API — one URL and model name change |
| Database | SQLite | PostgreSQL — connection string change only |
| ML model | Synthetic data | Retrain on real confirmed findings |

---

## About Troy & Banks

Troy & Banks has operated for 33 years reviewing utility bills for commercial clients across New York — hospitals, manufacturers, school districts, restaurant chains, and retail partners. They operate on a pure contingency model, earning only when they successfully recover billing overcharges.

---

*SUNY Buffalo · Team PM · github.com/SaiM1710/troy-banks-audit*
