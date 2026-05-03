import sqlite3
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
from sklearn.preprocessing import LabelEncoder

DB_NAME = "troy_banks_relational.db"
MIN_HISTORY = 12
ML_FEATURES = [
    "zscore_monthly", "zscore_overall",
    "pct_above_monthly", "pct_above_overall",
    "delivery_rate_ratio", "delivery_pct", "supply_pct",
    "demand_pct", "taxes_pct", "month",
    "industry_encoded", "utility_encoded",
]

conn = sqlite3.connect(DB_NAME)
df = pd.read_sql_query("""
    SELECT b.bill_id, b.utility_type, b.total_amount,
           b.usage_volume, b.is_anomaly_detected,
           b.billing_date, b.account_id,
           c.industry, v.vendor_name,
           COALESCE((SELECT SUM(li.total_price) FROM Line_Items li
                     WHERE li.bill_id = b.bill_id
                     AND li.category = 'Delivery Charge'), 0) as delivery_charge,
           COALESCE((SELECT SUM(li.total_price) FROM Line_Items li
                     WHERE li.bill_id = b.bill_id
                     AND li.category = 'Supply Charge'), 0) as supply_charge,
           COALESCE((SELECT SUM(li.total_price) FROM Line_Items li
                     WHERE li.bill_id = b.bill_id
                     AND li.category = 'Demand Charge'), 0) as demand_charge,
           COALESCE((SELECT SUM(li.total_price) FROM Line_Items li
                     WHERE li.bill_id = b.bill_id
                     AND li.category = 'Taxes and Surcharges'), 0) as taxes
    FROM Bills b
    JOIN Accounts a ON b.account_id = a.account_id
    JOIN Vendors v ON a.vendor_id = v.vendor_id
    JOIN Properties p ON a.property_id = p.property_id
    JOIN Clients c ON p.client_id = c.client_id
    WHERE b.total_amount > 0
    ORDER BY b.account_id, b.billing_date
""", conn)
conn.close()

df["billing_date"] = pd.to_datetime(df["billing_date"])
df["month"] = df["billing_date"].dt.month
df["year"]  = df["billing_date"].dt.year

le_i = LabelEncoder()
le_u = LabelEncoder()
df["industry_encoded"] = le_i.fit_transform(df["industry"])
df["utility_encoded"]  = le_u.fit_transform(df["utility_type"])

monthly_stats = (
    df.groupby(["account_id", "month"])["total_amount"]
    .agg(["mean", "std"])
    .reset_index()
    .rename(columns={"mean": "monthly_mean", "std": "monthly_std"})
)
df = df.merge(monthly_stats, on=["account_id", "month"], how="left")

overall_stats = (
    df.groupby("account_id")["total_amount"]
    .agg(["mean", "std"])
    .reset_index()
    .rename(columns={"mean": "overall_mean", "std": "overall_std"})
)
df = df.merge(overall_stats, on="account_id", how="left")

df["monthly_std"] = df["monthly_std"].fillna(1.0).replace(0, 1.0)
df["overall_std"] = df["overall_std"].fillna(1.0).replace(0, 1.0)

df["zscore_monthly"]    = (df["total_amount"] - df["monthly_mean"]) / df["monthly_std"]
df["zscore_overall"]    = (df["total_amount"] - df["overall_mean"]) / df["overall_std"]
df["pct_above_monthly"] = (df["total_amount"] - df["monthly_mean"]) / df["monthly_mean"] * 100
df["pct_above_overall"] = (df["total_amount"] - df["overall_mean"]) / df["overall_mean"] * 100

df["effective_delivery_rate"] = np.where(
    df["usage_volume"] > 0,
    df["delivery_charge"] / df["usage_volume"], 0
)
avg_rate = (
    df.groupby("account_id")["effective_delivery_rate"]
    .mean()
    .reset_index()
    .rename(columns={"effective_delivery_rate": "avg_delivery_rate"})
)
df = df.merge(avg_rate, on="account_id", how="left")
df["delivery_rate_ratio"] = np.where(
    df["avg_delivery_rate"] > 0,
    df["effective_delivery_rate"] / df["avg_delivery_rate"], 1.0
)

df["delivery_pct"] = np.where(df["total_amount"] > 0, df["delivery_charge"] / df["total_amount"] * 100, 0)
df["supply_pct"]   = np.where(df["total_amount"] > 0, df["supply_charge"]   / df["total_amount"] * 100, 0)
df["demand_pct"]   = np.where(df["total_amount"] > 0, df["demand_charge"]   / df["total_amount"] * 100, 0)
df["taxes_pct"]    = np.where(df["total_amount"] > 0, df["taxes"]           / df["total_amount"] * 100, 0)

history_count = (
    df.groupby("account_id")["bill_id"]
    .count()
    .reset_index()
    .rename(columns={"bill_id": "account_bill_count"})
)
df = df.merge(history_count, on="account_id", how="left")

eligible = df[df["account_bill_count"] >= MIN_HISTORY].copy()
X = eligible[ML_FEATURES].fillna(0)
y = eligible["is_anomaly_detected"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

model = RandomForestClassifier(
    n_estimators=100,
    class_weight="balanced",
    max_depth=10,
    min_samples_leaf=5,
    random_state=42,
    n_jobs=-1
)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]

print("=" * 55)
print("RANDOM FOREST MODEL EVALUATION")
print("=" * 55)

print("\n--- CLASSIFICATION REPORT ---")
print(classification_report(
    y_test, y_pred,
    target_names=["Normal Bill", "Anomalous Bill"]
))

print("--- CONFUSION MATRIX ---")
cm = confusion_matrix(y_test, y_pred)
tn, fp, fn, tp = cm.ravel()
print(f"                  Predicted Normal  Predicted Anomaly")
print(f"Actual Normal:    {tn:>16}  {fp:>17}")
print(f"Actual Anomaly:   {fn:>16}  {tp:>17}")
print()
print(f"True Negatives  (correctly identified normal):   {tn}")
print(f"False Positives (normal flagged as anomaly):     {fp}")
print(f"False Negatives (anomaly missed by model):       {fn}")
print(f"True Positives  (correctly caught anomalies):    {tp}")

print()
print("--- KEY METRICS ---")
roc = roc_auc_score(y_test, y_prob)
print(f"ROC-AUC Score:  {roc:.4f}")
print(f"  (1.0 = perfect, 0.5 = random guessing)")

cv_scores = cross_val_score(
    model, X, y, cv=5, scoring="f1", n_jobs=-1
)
print()
print("5-Fold Cross Validation F1 Scores:")
for i, score in enumerate(cv_scores, 1):
    print(f"  Fold {i}: {score:.4f}")
print(f"Mean F1: {cv_scores.mean():.4f} (+/- {cv_scores.std()*2:.4f})")

print()
print("--- BUSINESS IMPACT ---")
print(f"Bills in test set:          {len(y_test)}")
print(f"Real anomalies in test set: {int(y_test.sum())}")
print(f"Anomalies correctly caught: {tp}")
print(f"Anomalies missed:           {fn}")
print(f"False alarms raised:        {fp}")
print()
if y_test.sum() > 0:
    print(f"Miss rate:        {fn/y_test.sum()*100:.1f}% of real anomalies slipped through")
    print(f"Catch rate:       {tp/y_test.sum()*100:.1f}% of real anomalies detected")
if (tn + fp) > 0:
    print(f"False alarm rate: {fp/(tn+fp)*100:.1f}% of normal bills incorrectly flagged")
print("=" * 55)