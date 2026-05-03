"""
anomaly_detector.py — Multi-layer anomaly detection for Troy & Banks.

Detection Layers:
  Layer 1 — Math Audit (already runs during ingestion in llm_parser.py)
  Layer 2 — Random Forest ML (trains on historical labeled bills)
  Layer 3 — Weather Normalization (Open-Meteo API — HDD/CDD)

Run with:
  python anomaly_detector.py
"""

import sqlite3
import json
import math
import requests
import numpy as np
import pandas as pd
from datetime import datetime, date
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.preprocessing import LabelEncoder

DB_NAME = "troy_banks_relational.db"

# ── INDUSTRY WEATHER SENSITIVITY ─────────────────────────────────────────────
# How much of a bill's variation is driven by weather vs operations.
# 1.0 = entirely weather driven, 0.0 = weather irrelevant
WEATHER_SENSITIVITY = {
    "Healthcare":    {"gas": 0.4, "electric": 0.3},
    "Manufacturing": {"gas": 0.2, "electric": 0.2},
    "Food Service":  {"gas": 0.3, "electric": 0.5},
    "Education":     {"gas": 0.9, "electric": 0.6},
    "Retail":        {"gas": 0.7, "electric": 0.7},
}

# ── SEVERITY THRESHOLDS (based on potential recovery amount) ──────────────────
SEVERITY_THRESHOLDS = {
    "URGENT": 5000,
    "HIGH":   1000,
    "MEDIUM":  200,
    "LOW":       0,
}

# ── MINIMUM HISTORY REQUIRED BEFORE ML FLAGS AN ACCOUNT ──────────────────────
MIN_HISTORY_BILLS = 12

# ── BUFFALO COORDINATES (covers all synthetic client properties) ──────────────
DEFAULT_LAT = 42.89
DEFAULT_LON = -78.86

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_NAME, timeout=10.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def load_all_bills() -> pd.DataFrame:
    """
    Load all bills from the database with their account,
    property, client, and vendor context.
    """
    conn = get_conn()
    query = """
        SELECT
            b.bill_id,
            b.account_id,
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
            a.account_number,
            v.vendor_name,
            p.city,
            p.state,
            c.client_name,
            c.industry,
            COALESCE(
                (SELECT SUM(li.total_price)
                 FROM Line_Items li
                 WHERE li.bill_id = b.bill_id
                 AND li.category = 'Delivery Charge'), 0
            ) as delivery_charge,
            COALESCE(
                (SELECT SUM(li.total_price)
                 FROM Line_Items li
                 WHERE li.bill_id = b.bill_id
                 AND li.category = 'Supply Charge'), 0
            ) as supply_charge,
            COALESCE(
                (SELECT SUM(li.total_price)
                 FROM Line_Items li
                 WHERE li.bill_id = b.bill_id
                 AND li.category = 'Demand Charge'), 0
            ) as demand_charge,
            COALESCE(
                (SELECT SUM(li.total_price)
                 FROM Line_Items li
                 WHERE li.bill_id = b.bill_id
                 AND li.category = 'Taxes and Surcharges'), 0
            ) as taxes
        FROM Bills b
        JOIN Accounts a ON b.account_id = a.account_id
        JOIN Vendors v ON a.vendor_id = v.vendor_id
        JOIN Properties p ON a.property_id = p.property_id
        JOIN Clients c ON p.client_id = c.client_id
        WHERE b.total_amount > 0
        ORDER BY b.account_id, b.billing_date
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Parse dates
    df['billing_date'] = pd.to_datetime(df['billing_date'], errors='coerce')
    df['month'] = df['billing_date'].dt.month
    df['year'] = df['billing_date'].dt.year

    print(f"[DATA] Loaded {len(df)} bills across "
          f"{df['account_id'].nunique()} accounts "
          f"and {df['client_name'].nunique()} clients.")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# FEATURE ENGINEERING
# ─────────────────────────────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build relative features for each bill.
    Uses percentages and ratios — not absolute values —
    so the model generalizes to new accounts of any size.
    """
    print("\n[FEATURES] Engineering features...")

    # Encode categorical columns
    le_industry = LabelEncoder()
    le_utility = LabelEncoder()
    le_vendor = LabelEncoder()

    df['industry_encoded']  = le_industry.fit_transform(df['industry'])
    df['utility_encoded']   = le_utility.fit_transform(df['utility_type'])
    df['vendor_encoded']    = le_vendor.fit_transform(df['vendor_name'])

    # ── ACCOUNT-LEVEL BASELINES ───────────────────────────────────────────────
    # Per-month baseline for each account
    monthly_stats = (
        df.groupby(['account_id', 'month'])['total_amount']
        .agg(['mean', 'std'])
        .reset_index()
        .rename(columns={'mean': 'monthly_mean', 'std': 'monthly_std'})
    )
    df = df.merge(monthly_stats, on=['account_id', 'month'], how='left')

    # Overall baseline for each account
    overall_stats = (
        df.groupby('account_id')['total_amount']
        .agg(['mean', 'std'])
        .reset_index()
        .rename(columns={'mean': 'overall_mean', 'std': 'overall_std'})
    )
    df = df.merge(overall_stats, on='account_id', how='left')

    # Fill any zero std dev with small value to avoid division by zero
    df['monthly_std'] = df['monthly_std'].fillna(1.0).replace(0, 1.0)
    df['overall_std'] = df['overall_std'].fillna(1.0).replace(0, 1.0)

    # ── STATISTICAL FEATURES ─────────────────────────────────────────────────
    df['zscore_monthly'] = (
        (df['total_amount'] - df['monthly_mean']) / df['monthly_std']
    )
    df['zscore_overall'] = (
        (df['total_amount'] - df['overall_mean']) / df['overall_std']
    )
    df['pct_above_monthly'] = (
        (df['total_amount'] - df['monthly_mean']) / df['monthly_mean'] * 100
    ).fillna(0)
    df['pct_above_overall'] = (
        (df['total_amount'] - df['overall_mean']) / df['overall_mean'] * 100
    ).fillna(0)

    # ── RATE FEATURES ────────────────────────────────────────────────────────
    # Effective delivery rate per unit
    df['effective_delivery_rate'] = np.where(
        df['usage_volume'] > 0,
        df['delivery_charge'] / df['usage_volume'],
        0
    )

    # Historical effective delivery rate per account
    rate_stats = (
        df[df['usage_volume'] > 0]
        .groupby('account_id')['effective_delivery_rate']
        .mean()
        .reset_index()
        .rename(columns={'effective_delivery_rate': 'avg_delivery_rate'})
    )
    df = df.merge(rate_stats, on='account_id', how='left')

    df['delivery_rate_ratio'] = np.where(
        df['avg_delivery_rate'] > 0,
        df['effective_delivery_rate'] / df['avg_delivery_rate'],
        1.0
    )

    # ── CHARGE COMPOSITION FEATURES ──────────────────────────────────────────
    # Each charge as % of total — catches composition anomalies
    df['delivery_pct'] = np.where(
        df['total_amount'] > 0,
        df['delivery_charge'] / df['total_amount'] * 100, 0
    )
    df['supply_pct'] = np.where(
        df['total_amount'] > 0,
        df['supply_charge'] / df['total_amount'] * 100, 0
    )
    df['demand_pct'] = np.where(
        df['total_amount'] > 0,
        df['demand_charge'] / df['total_amount'] * 100, 0
    )
    df['taxes_pct'] = np.where(
        df['total_amount'] > 0,
        df['taxes'] / df['total_amount'] * 100, 0
    )

    # ── HISTORY COUNT ────────────────────────────────────────────────────────
    # How many bills exist for this account — used for minimum history gate
    history_count = (
        df.groupby('account_id')['bill_id']
        .count()
        .reset_index()
        .rename(columns={'bill_id': 'account_bill_count'})
    )
    df = df.merge(history_count, on='account_id', how='left')

    print(f"[FEATURES] Engineered {len(df.columns)} total columns.")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# QUICK TEST
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# RANDOM FOREST — TRAINING AND PREDICTION
# ─────────────────────────────────────────────────────────────────────────────

# Features the model uses — all relative, no absolute dollar amounts
ML_FEATURES = [
    'zscore_monthly',        # how unusual vs same month history
    'zscore_overall',        # how unusual vs all account history
    'pct_above_monthly',     # % above monthly average
    'pct_above_overall',     # % above overall average
    'delivery_rate_ratio',   # effective rate vs account historical rate
    'delivery_pct',          # delivery as % of total
    'supply_pct',            # supply as % of total
    'demand_pct',            # demand as % of total
    'taxes_pct',             # taxes as % of total
    'month',                 # seasonality
    'industry_encoded',      # industry type
    'utility_encoded',       # electric or gas
]


def train_random_forest(df: pd.DataFrame):
    """
    Train Random Forest on all labeled bills.
    Returns trained model and feature importance dict.
    """
    print("\n[ML] Training Random Forest classifier...")

    # Only train on accounts with enough history
    eligible = df[df['account_bill_count'] >= MIN_HISTORY_BILLS].copy()
    print(f"[ML] Eligible bills for training: {len(eligible)} "
          f"({len(df) - len(eligible)} excluded — insufficient history)")

    X = eligible[ML_FEATURES].fillna(0)
    y = eligible['is_anomaly_detected']

    # Train/test split — stratified to preserve anomaly ratio
    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )

    # Train with balanced class weights to handle 15% anomaly rate
    model = RandomForestClassifier(
        n_estimators=100,
        class_weight='balanced',
        max_depth=10,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    # Evaluate on test set
    y_pred = model.predict(X_test)
    print("\n[ML] Model Performance on Test Set:")
    print(classification_report(
        y_test, y_pred,
        target_names=['Normal', 'Anomaly']
    ))

    # Feature importance — what the model learned matters most
    importance = dict(zip(ML_FEATURES, model.feature_importances_))
    importance_sorted = dict(
        sorted(importance.items(), key=lambda x: x[1], reverse=True)
    )

    print("\n[ML] Feature Importance (what drives anomaly detection):")
    feature_labels = {
        'zscore_monthly':      'Historical monthly comparison',
        'zscore_overall':      'Overall account comparison',
        'pct_above_monthly':   'Percentage above monthly average',
        'pct_above_overall':   'Percentage above overall average',
        'delivery_rate_ratio': 'Delivery rate change',
        'delivery_pct':        'Delivery charge proportion',
        'supply_pct':          'Supply charge proportion',
        'demand_pct':          'Demand charge proportion',
        'taxes_pct':           'Taxes proportion',
        'month':               'Seasonality',
        'industry_encoded':    'Industry type',
        'utility_encoded':     'Utility type',
    }
    for feature, score in importance_sorted.items():
        label = feature_labels.get(feature, feature)
        bar = '█' * int(score * 100)
        print(f"  {label:<35} {score:.3f}  {bar}")

    return model, importance_sorted


def predict_anomalies(
    df: pd.DataFrame,
    model: RandomForestClassifier
) -> pd.DataFrame:
    """
    Run model on all eligible bills and add probability scores.
    Bills with insufficient history get probability = 0.0 (not flagged by ML).
    """
    print("\n[ML] Running predictions on all bills...")

    df['ml_anomaly_probability'] = 0.0
    df['ml_flagged'] = False

    # Only predict on accounts with enough history
    eligible_mask = df['account_bill_count'] >= MIN_HISTORY_BILLS
    eligible = df[eligible_mask].copy()

    if len(eligible) == 0:
        print("[ML] No eligible accounts found for prediction.")
        return df

    X = eligible[ML_FEATURES].fillna(0)
    probabilities = model.predict_proba(X)[:, 1]  # probability of anomaly

    df.loc[eligible_mask, 'ml_anomaly_probability'] = probabilities

    # Flag if probability exceeds threshold
    threshold = 0.60  # 60% confidence to flag
    df.loc[eligible_mask, 'ml_flagged'] = probabilities >= threshold

    flagged_count = df['ml_flagged'].sum()
    print(f"[ML] Bills flagged by Random Forest: {flagged_count} "
          f"({flagged_count/len(df)*100:.1f}%)")
    print(f"[ML] Bills skipped (insufficient history): "
          f"{(~eligible_mask).sum()}")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# WEATHER LAYER — OPEN-METEO API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_weather(
    start_date: str,
    end_date: str,
    lat: float = DEFAULT_LAT,
    lon: float = DEFAULT_LON
) -> dict:
    """
    Fetch daily temperature data from Open-Meteo historical archive.
    Free API — no key required.
    Returns dict with HDD, CDD, and average temperature for the period.
    """
    try:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude":         lat,
            "longitude":        lon,
            "start_date":       start_date,
            "end_date":         end_date,
            "daily":            "temperature_2m_max,temperature_2m_min",
            "temperature_unit": "fahrenheit",
            "timezone":         "America/New_York"
        }
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()

        daily = data.get("daily", {})
        temps_max = daily.get("temperature_2m_max", [])
        temps_min = daily.get("temperature_2m_min", [])

        if not temps_max or not temps_min:
            return None

        # Calculate HDD and CDD for each day
        # Base temperature 65°F — industry standard
        BASE_TEMP = 65.0
        hdd_total = 0.0
        cdd_total = 0.0
        avg_temps = []

        for tmax, tmin in zip(temps_max, temps_min):
            if tmax is None or tmin is None:
                continue
            avg = (tmax + tmin) / 2
            avg_temps.append(avg)
            hdd_total += max(0, BASE_TEMP - avg)
            cdd_total += max(0, avg - BASE_TEMP)

        return {
            "hdd":      round(hdd_total, 1),
            "cdd":      round(cdd_total, 1),
            "avg_temp": round(sum(avg_temps) / len(avg_temps), 1)
                        if avg_temps else None,
            "days":     len(avg_temps)
        }

    except Exception as e:
        return None


def build_weather_baselines(df: pd.DataFrame) -> dict:
    """
    Build historical weather baselines for each month.
    Fetches actual weather data for each year-month in the dataset
    and calculates the average HDD/CDD for each calendar month.

    Returns dict: {month: {'avg_hdd': X, 'avg_cdd': Y}}
    """
    print("\n[WEATHER] Building historical weather baselines...")
    print("[WEATHER] Fetching from Open-Meteo API (this may take a moment)...")

    monthly_weather = {}  # {(year, month): {hdd, cdd}}

    # Get unique year-month combinations from the data
    year_months = df[['year', 'month']].drop_duplicates().sort_values(
        ['year', 'month']
    )

    fetched = 0
    failed = 0

    for _, row in year_months.iterrows():
        year = int(row['year'])
        month = int(row['month'])

        # Build date range for that month
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        start = f"{year}-{month:02d}-01"
        end   = f"{year}-{month:02d}-{last_day:02d}"

        weather = fetch_weather(start, end)
        if weather:
            monthly_weather[(year, month)] = weather
            fetched += 1
        else:
            failed += 1

    print(f"[WEATHER] Fetched {fetched} months, failed {failed} months.")

    # Calculate historical average HDD/CDD per calendar month
    baselines = {}
    for month_num in range(1, 13):
        month_data = [
            v for (y, m), v in monthly_weather.items()
            if m == month_num
        ]
        if month_data:
            baselines[month_num] = {
                "avg_hdd": round(
                    sum(d['hdd'] for d in month_data) / len(month_data), 1
                ),
                "avg_cdd": round(
                    sum(d['cdd'] for d in month_data) / len(month_data), 1
                ),
            }

    print("[WEATHER] Monthly baselines calculated:")
    month_names = ['Jan','Feb','Mar','Apr','May','Jun',
                   'Jul','Aug','Sep','Oct','Nov','Dec']
    for m, stats in baselines.items():
        print(f"  {month_names[m-1]}: "
              f"HDD={stats['avg_hdd']:6.1f}  "
              f"CDD={stats['avg_cdd']:6.1f}")

    return monthly_weather, baselines


def apply_weather_context(
    row: pd.Series,
    monthly_weather: dict,
    baselines: dict
) -> dict:
    """
    For a single bill, check whether its usage is explained by weather.
    Returns a dict with weather context fields.
    """
    year  = int(row['year'])
    month = int(row['month'])
    industry = row['industry']
    utility  = row['utility_type'].lower()

    # Get actual weather for this billing period
    actual_weather = monthly_weather.get((year, month))
    baseline       = baselines.get(month)

    if not actual_weather or not baseline:
        return {
            "weather_available":    False,
            "weather_explains":     None,
            "weather_actual":       0.0,
            "weather_historical":   0.0,
            "weather_deviation_pct": 0.0,
        }

    # Use HDD for gas, CDD for electric
    if utility == "gas":
        actual_deg_days     = actual_weather['hdd']
        historical_deg_days = baseline['avg_hdd']
        metric_name = "HDD"
    else:
        actual_deg_days     = actual_weather['cdd']
        historical_deg_days = baseline['avg_cdd']
        metric_name = "CDD"

    # Avoid division by zero for months with near-zero degree days
    if historical_deg_days < 5:
        weather_deviation_pct = 0.0
    else:
        weather_deviation_pct = (
            (actual_deg_days - historical_deg_days)
            / historical_deg_days * 100
        )

    # Get industry weather sensitivity
    sensitivity_map = WEATHER_SENSITIVITY.get(industry, {"gas": 0.5, "electric": 0.5})
    sensitivity = sensitivity_map.get(utility, 0.5)

    # Weather explains the usage spike if:
    # - Weather was more extreme than normal (positive deviation)
    # - AND the industry is sensitive to weather
    # - AND the weather deviation is proportional to the usage deviation
    usage_deviation_pct = float(row.get('pct_above_monthly', 0))

    weather_explains = False
    if actual_deg_days > 0 and historical_deg_days > 0:
        # Expected usage increase due to weather
        expected_usage_increase = weather_deviation_pct * sensitivity
        # If weather accounts for most of the usage increase → explains it
        if (weather_deviation_pct > 10
                and expected_usage_increase >= usage_deviation_pct * 0.6):
            weather_explains = True

    return {
        "weather_available":     True,
        "weather_explains":      weather_explains,
        "weather_actual":        actual_deg_days,
        "weather_historical":    historical_deg_days,
        "weather_deviation_pct": round(weather_deviation_pct, 1),
        "metric_name":           metric_name,
    }



# ─────────────────────────────────────────────────────────────────────────────
# PLAIN ENGLISH GENERATION
# ─────────────────────────────────────────────────────────────────────────────

MONTH_NAMES = [
    'January', 'February', 'March', 'April', 'May', 'June',
    'July', 'August', 'September', 'October', 'November', 'December'
]


def get_severity(potential_recovery: float) -> str:
    """Determine severity based on potential recovery amount."""
    if potential_recovery >= SEVERITY_THRESHOLDS['URGENT']:
        return 'URGENT'
    elif potential_recovery >= SEVERITY_THRESHOLDS['HIGH']:
        return 'HIGH'
    elif potential_recovery >= SEVERITY_THRESHOLDS['MEDIUM']:
        return 'MEDIUM'
    return 'LOW'


def generate_plain_english(
    row: pd.Series,
    weather: dict,
    detection_layer: str
) -> tuple:
    """
    Generate a plain English explanation an auditor can read and act on.
    No jargon. No Z-scores. Just what happened and why it matters.
    Returns (reason, finding_type, potential_recovery)
    """
    month_name  = MONTH_NAMES[int(row['month']) - 1]
    utility     = row['utility_type']
    actual      = float(row['total_amount'])
    expected    = float(row['monthly_mean'])
    pct_above   = float(row['pct_above_monthly'])
    recovery    = max(0.0, actual - expected)
    years       = max(1, int(row['account_bill_count']) // 12)

    # ── MATH AUDIT FINDING ───────────────────────────────────────────────────
    if detection_layer == 'MATH':
        return (
            f"The charges on this bill do not add up correctly. "
            f"The individual line items sum to a different amount "
            f"than the total shown. This is a calculation error "
            f"that can be disputed directly with the vendor.",
            'MATH_ERROR',
            recovery
        )

    # ── DETERMINE DIRECTION — spike or drop ──────────────────────────────────
    is_spike = pct_above > 0
    direction = "higher" if is_spike else "lower"
    pct_display = abs(pct_above)

    # ── WEATHER CONTEXT ───────────────────────────────────────────────────────
    weather_sentence = ""
    if weather and weather.get('weather_available'):
        metric    = weather.get('metric_name', 'degree days')
        w_actual  = weather.get('weather_actual', 0)
        w_hist    = weather.get('weather_historical', 0)
        w_dev     = weather.get('weather_deviation_pct', 0)
        explains  = weather.get('weather_explains', False)

        if explains:
            weather_sentence = (
                f" The weather during this billing period was "
                f"{abs(w_dev):.0f}% more extreme than the historical "
                f"average for {month_name} ({w_actual:.0f} vs "
                f"{w_hist:.0f} {metric}), which explains some of "
                f"the higher usage."
            )
            finding_type = 'WEATHER_DRIVEN'
        else:
            if w_actual < w_hist * 0.8 and is_spike:
                # Weather was MILDER than normal but bill is HIGH
                weather_sentence = (
                    f" The weather during this period was actually "
                    f"milder than usual for {month_name} "
                    f"({w_actual:.0f} vs {w_hist:.0f} {metric} historical "
                    f"average), so weather does not explain this spike. "
                    f"This makes the high bill more suspicious."
                )
                finding_type = 'WEATHER_UNEXPLAINED_SPIKE'
            elif w_actual > w_hist * 1.2 and not is_spike:
                # Weather was MORE extreme but bill is LOW
                weather_sentence = (
                    f" The weather was more extreme than usual for "
                    f"{month_name} ({w_actual:.0f} vs {w_hist:.0f} "
                    f"{metric} historical average), yet usage was lower "
                    f"than expected. This may indicate a meter issue."
                )
                finding_type = 'WEATHER_UNEXPLAINED_DROP'
            else:
                weather_sentence = (
                    f" Weather conditions were normal for {month_name} "
                    f"and do not explain this unusual bill."
                )
                finding_type = (
                    'USAGE_SPIKE' if is_spike else 'USAGE_DROP'
                )
    else:
        finding_type = 'USAGE_SPIKE' if is_spike else 'USAGE_DROP'

    # ── BUILD THE FULL REASON ─────────────────────────────────────────────────
    if is_spike:
        reason = (
            f"This {utility.lower()} bill of ${actual:,.0f} is "
            f"{pct_display:.0f}% {direction} than the average "
            f"{month_name} bill for this account "
            f"(${expected:,.0f} average over {years} "
            f"{'year' if years == 1 else 'years'})."
            f"{weather_sentence}"
        )
    else:
        reason = (
            f"This {utility.lower()} bill of ${actual:,.0f} is "
            f"{pct_display:.0f}% {direction} than the average "
            f"{month_name} bill for this account "
            f"(${expected:,.0f} average over {years} "
            f"{'year' if years == 1 else 'years'}). "
            f"An unusually low bill may indicate a meter malfunction "
            f"or service interruption that should be investigated."
            f"{weather_sentence}"
        )

    return reason, finding_type, recovery


# ─────────────────────────────────────────────────────────────────────────────
# STORE FINDINGS IN DATABASE
# ─────────────────────────────────────────────────────────────────────────────

def store_finding(
    conn,
    bill_id:        int,
    detection_layer: str,
    finding_type:   str,
    severity:       str,
    expected:       float,
    actual:         float,
    deviation_pct:  float,
    zscore_monthly: float,
    zscore_overall: float,
    weather_actual: float,
    weather_hist:   float,
    probability:    float,
    reason:         str,
    recovery:       float,
):
    """Insert one finding into Anomaly_Analysis table."""
    conn.execute('''
        INSERT INTO Anomaly_Analysis (
            bill_id, detection_layer, finding_type, severity,
            expected_value, actual_value, deviation_pct,
            zscore_monthly, zscore_overall,
            weather_actual, weather_historical,
            anomaly_probability, plain_english_reason,
            potential_recovery, detected_at, reviewed
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
    ''', (
        bill_id, detection_layer, finding_type, severity,
        round(expected, 2), round(actual, 2), round(deviation_pct, 2),
        round(zscore_monthly, 3), round(zscore_overall, 3),
        round(weather_actual, 1), round(weather_hist, 1),
        round(probability, 4), reason,
        round(recovery, 2),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN DETECTION RUNNER
# ─────────────────────────────────────────────────────────────────────────────

def run_detection(
    df:             pd.DataFrame,
    model:          RandomForestClassifier,
    monthly_weather: dict,
    baselines:      dict,
):
    """
    Run full anomaly detection on all bills and store findings.
    """
    print("\n" + "="*60)
    print("[DETECTOR] Starting full anomaly detection run...")
    print("="*60)

    conn = get_conn()
    conn.execute("DELETE FROM Anomaly_Analysis")
    conn.commit()
    print("[DETECTOR] Cleared previous findings.")

    # Load actual math audit reasons from DB
    math_reasons = {}
    for row_db in conn.execute(
        "SELECT bill_id, anomaly_reason FROM Bills "
        "WHERE is_anomaly_detected = 1"
    ).fetchall():
        math_reasons[row_db['bill_id']] = row_db['anomaly_reason'] or ""

    total_findings = 0
    urgent_count   = 0
    high_count     = 0
    total_recovery = 0.0

    import re

    for _, row in df.iterrows():
        bill_id      = int(row['bill_id'])
        ml_prob      = float(row.get('ml_anomaly_probability', 0.0))
        ml_flagged   = bool(row.get('ml_flagged', False))
        has_history  = int(row['account_bill_count']) >= MIN_HISTORY_BILLS
        stored_reason = math_reasons.get(bill_id, "")

        # Is this a REAL math audit failure (actual discrepancy)?
        real_math_error = 'MATH AUDIT FAILED' in stored_reason
        match = re.search(r'Discrepancy of \$([0-9]+(?:\.[0-9]+)?)', stored_reason)
        math_recovery = float(match.group(1)) if match else 0.0

        # Get weather context
        weather = apply_weather_context(row, monthly_weather, baselines)

        findings_for_bill = []

        # ── LAYER 1: REAL MATH ERROR ─────────────────────────────────────────
        if real_math_error and math_recovery > 0.01:
            severity = get_severity(math_recovery)
            reason = (
                f"The charges on this {row['utility_type'].lower()} bill "
                f"do not add up correctly. The individual line items sum "
                f"to a different amount than the total shown on the bill "
                f"(discrepancy of ${math_recovery:,.2f}). "
                f"This is a calculation error that can be disputed "
                f"directly with the vendor."
            )
            findings_for_bill.append({
                'layer':    'MATH',
                'type':     'MATH_ERROR',
                'recovery': math_recovery,
                'reason':   reason,
                'prob':     1.0,
                'severity': severity,
            })

        # ── LAYER 2 + 3: ML + WEATHER ────────────────────────────────────────
        elif ml_flagged and has_history:
            reason, ftype, recovery = generate_plain_english(
                row, weather, 'ML'
            )

            # Combined if weather also confirms
            if (weather.get('weather_available')
                    and not weather.get('weather_explains')
                    and abs(float(row.get('pct_above_monthly', 0))) > 20):
                layer = 'COMBINED'
            else:
                layer = 'ML'

            severity = get_severity(recovery)
            findings_for_bill.append({
                'layer':    layer,
                'type':     ftype,
                'recovery': recovery,
                'reason':   reason,
                'prob':     ml_prob,
                'severity': severity,
            })

        # ── STORE FINDINGS ───────────────────────────────────────────────────
        for f in findings_for_bill:
            store_finding(
                conn=conn,
                bill_id=bill_id,
                detection_layer=f['layer'],
                finding_type=f['type'],
                severity=f['severity'],
                expected=float(row['monthly_mean']),
                actual=float(row['total_amount']),
                deviation_pct=float(row.get('pct_above_monthly', 0)),
                zscore_monthly=float(row.get('zscore_monthly', 0)),
                zscore_overall=float(row.get('zscore_overall', 0)),
                weather_actual=weather.get('weather_actual', 0.0),
                weather_hist=weather.get('weather_historical', 0.0),
                probability=f['prob'],
                reason=f['reason'],
                recovery=f['recovery'],
            )

            total_findings += 1
            total_recovery += f['recovery']
            if f['severity'] == 'URGENT':
                urgent_count += 1
            elif f['severity'] == 'HIGH':
                high_count += 1

    conn.commit()
    conn.close()

    print(f"\n[DETECTOR] Detection complete.")
    print(f"  Total findings stored:    {total_findings}")
    print(f"  Urgent findings:          {urgent_count}")
    print(f"  High priority findings:   {high_count}")
    print(f"  Total potential recovery: ${total_recovery:,.2f}")

    return total_findings, total_recovery

# ─────────────────────────────────────────────────────────────────────────────
# AUDITOR REPORT
# ─────────────────────────────────────────────────────────────────────────────

def print_auditor_report():
    """
    Print a plain English summary grouped by client then severity.
    This is what the auditor sees when they start their day.
    """
    conn = get_conn()

    print("\n" + "="*60)
    print("  TROY & BANKS — DAILY ANOMALY REPORT")
    print("="*60)

    # Get all clients with findings
    clients = conn.execute("""
        SELECT DISTINCT c.client_name, c.industry
        FROM Anomaly_Analysis aa
        JOIN Bills b ON aa.bill_id = b.bill_id
        JOIN Accounts a ON b.account_id = a.account_id
        JOIN Properties p ON a.property_id = p.property_id
        JOIN Clients c ON p.client_id = c.client_id
        WHERE aa.reviewed = 0
        ORDER BY c.client_name
    """).fetchall()

    grand_total_recovery = 0.0

    for client in clients:
        client_name = client['client_name']

        # Get findings for this client grouped by severity
        findings = conn.execute("""
            SELECT
                aa.analysis_id,
                aa.severity,
                aa.finding_type,
                aa.plain_english_reason,
                aa.potential_recovery,
                aa.detection_layer,
                b.utility_type,
                b.billing_date,
                b.total_amount,
                p.property_name,
                v.vendor_name
            FROM Anomaly_Analysis aa
            JOIN Bills b ON aa.bill_id = b.bill_id
            JOIN Accounts a ON b.account_id = a.account_id
            JOIN Properties p ON a.property_id = p.property_id
            JOIN Clients c ON p.client_id = c.client_id
            JOIN Vendors v ON a.vendor_id = v.vendor_id
            WHERE c.client_name = ?
            AND aa.reviewed = 0
            ORDER BY
                CASE aa.severity
                    WHEN 'URGENT' THEN 1
                    WHEN 'HIGH'   THEN 2
                    WHEN 'MEDIUM' THEN 3
                    WHEN 'LOW'    THEN 4
                END,
                aa.potential_recovery DESC
        """, (client_name,)).fetchall()

        if not findings:
            continue

        client_recovery = sum(f['potential_recovery'] for f in findings)
        grand_total_recovery += client_recovery

        print(f"\n{'─'*60}")
        print(f"  {client_name.upper()}")
        print(f"  {len(findings)} findings  |  "
              f"Potential recovery: ${client_recovery:,.0f}")
        print(f"{'─'*60}")

        severity_icons = {
            'URGENT': '🔴 URGENT',
            'HIGH':   '🟡 HIGH PRIORITY',
            'MEDIUM': '🟠 WORTH REVIEWING',
            'LOW':    '🟢 MINOR'
        }

        current_severity = None
        for f in findings:
            if f['severity'] != current_severity:
                current_severity = f['severity']
                icon = severity_icons.get(current_severity, current_severity)
                print(f"\n  {icon}")

            date_str = f['billing_date'][:7] if f['billing_date'] else 'Unknown'
            print(f"\n  [{f['analysis_id']}] "
                  f"{f['property_name']} — "
                  f"{f['utility_type']} — "
                  f"{f['vendor_name']} — "
                  f"{date_str}")
            print(f"  Bill total: ${f['total_amount']:,.0f}  |  "
                  f"Potential overcharge: ${f['potential_recovery']:,.0f}")
            print(f"  {f['plain_english_reason']}")

    print(f"\n{'='*60}")
    print(f"  TOTAL POTENTIAL RECOVERY ACROSS ALL CLIENTS:")
    print(f"  ${grand_total_recovery:,.0f}")
    print(f"{'='*60}\n")

    conn.close()

if __name__ == "__main__":
    print("\n" + "="*60)
    print("TROY & BANKS — ANOMALY DETECTOR")
    print("Full pipeline run")
    print("="*60)

    # Step A — Load and engineer features
    df = load_all_bills()
    df = engineer_features(df)

    # Step B — Train Random Forest and predict
    model, importance = train_random_forest(df)
    df = predict_anomalies(df, model)

    # Step C — Build weather baselines
    monthly_weather, baselines = build_weather_baselines(df)

    # Step D — Run full detection and store findings
    total_findings, total_recovery = run_detection(
        df, model, monthly_weather, baselines
    )

    # Print auditor report
    print_auditor_report()