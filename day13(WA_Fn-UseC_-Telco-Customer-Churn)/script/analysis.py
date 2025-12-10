# analysis.py

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import seaborn as sns


# ---------------- Supabase connection ----------------
def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("Missing SUPABASE_URL or SUPABASE_KEY in .env")
    return create_client(url, key)


# ---------------- Fetch churn_data table ----------------
def fetch_churn_data():
    supabase = get_supabase_client()
    result = supabase.table("churn_data").select("*").execute()
    df = pd.DataFrame(result.data)
    df.columns = df.columns.str.lower()  # lowercase columns
    return df


# ---------------- Generate metrics ----------------
def generate_metrics(df: pd.DataFrame):
    metrics = {}

    # Churn percentage
    churn_count = df['churn'].str.lower().value_counts().get('yes', 0)
    metrics['churn_percentage'] = round((churn_count / len(df)) * 100, 2)

    # Average monthly charges per contract type
    metrics['avg_monthly_charges_per_contract'] = df.groupby('contract')['monthlycharges'].mean().round(2).to_dict()

    # Count of customers by tenure group
    metrics['tenure_group_counts'] = df['tenure_group'].value_counts().to_dict()

    # Internet service distribution
    metrics['internet_service_distribution'] = df['internetservice'].value_counts().to_dict()

    # Pivot table: churn vs tenure group
    pivot = pd.pivot_table(
        df,
        index='tenure_group',
        columns='churn',
        values='id',
        aggfunc='count',
        fill_value=0
    )
    metrics['churn_vs_tenure_pivot'] = pivot

    return metrics


# ---------------- Optional visualizations ----------------
def generate_visualizations(df: pd.DataFrame, output_dir="..\\data\\processed\\figures"):
    os.makedirs(output_dir, exist_ok=True)

    # Map churn to numeric for plotting
    df['churn_numeric'] = df['churn'].map({'Yes': 1, 'No': 0})

    # Churn rate by monthly charge segment
    plt.figure(figsize=(6,4))
    sns.barplot(x='monthly_charge_segment', y='churn_numeric', data=df)
    plt.ylabel("Churn Rate")
    plt.title("Churn Rate by Monthly Charge Segment")
    plt.savefig(os.path.join(output_dir, "churn_by_monthly_segment.png"))
    plt.close()

    # Histogram of total charges
    plt.figure(figsize=(6,4))
    sns.histplot(df['totalcharges'].dropna(), bins=30, kde=True)
    plt.title("Histogram of TotalCharges")
    plt.xlabel("TotalCharges")
    plt.ylabel("Count")
    plt.savefig(os.path.join(output_dir, "totalcharges_histogram.png"))
    plt.close()

    # Bar plot of contract types
    plt.figure(figsize=(6,4))
    sns.countplot(x='contract', data=df)
    plt.title("Count of Contract Types")
    plt.xlabel("Contract")
    plt.ylabel("Count")
    plt.savefig(os.path.join(output_dir, "contract_type_counts.png"))
    plt.close()


# ---------------- Save summary CSV ----------------
def save_summary_csv(df: pd.DataFrame, output_path="..\\data\\processed\\analysis_summary.csv"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    summary_cols = [
        'tenure', 'monthlycharges', 'totalcharges', 'churn',
        'tenure_group', 'monthly_charge_segment', 'contract', 'internetservice'
    ]
    df[summary_cols].to_csv(output_path, index=False)
    print(f"✅ Analysis summary saved at: {output_path}")


# ---------------- Main ETL function ----------------
if __name__ == "__main__":
    print("📌 Fetching churn_data from Supabase...")
    df = fetch_churn_data()
    print(f"📊 Fetched {len(df)} rows.")

    # Warn if Supabase rows < local dataset
    local_csv = os.path.join("..","data","staged","churn_transformed.csv")
    if os.path.exists(local_csv):
        local_rows = len(pd.read_csv(local_csv))
        if len(df) < local_rows:
            print(f"⚠️ Warning: Supabase table has fewer rows ({len(df)}) than local dataset ({local_rows})")

    print("🔍 Generating metrics...")
    metrics = generate_metrics(df)
    print("\n📈 Metrics summary:")
    print(f"Churn Percentage: {metrics['churn_percentage']}%")
    print("Avg Monthly Charges per Contract:", metrics['avg_monthly_charges_per_contract'])
    print("Tenure Group Counts:", metrics['tenure_group_counts'])
    print("Internet Service Distribution:", metrics['internet_service_distribution'])
    print("\nPivot Table: Churn vs Tenure Group")
    print(metrics['churn_vs_tenure_pivot'].to_string())

    print("\n🎨 Generating visualizations...")
    generate_visualizations(df)

    print("💾 Saving analysis summary CSV...")
    save_summary_csv(df)

    print("\n✅ ETL analysis completed successfully!")
