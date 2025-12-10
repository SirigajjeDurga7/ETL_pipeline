import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env file")

    return create_client(url, key)

def validate_data(local_csv_path: str, table_name="churn_data"):
    if not os.path.exists(local_csv_path):
        print(f"❌ File not found: {local_csv_path}")
        return

    # Load original dataset
    df = pd.read_csv(local_csv_path)
    df.columns = df.columns.str.lower()

    # Keep only columns in Supabase table
    table_columns = [
        "tenure", "monthlycharges", "totalcharges", "churn",
        "internetservice", "contract", "paymentmethod",
        "tenure_group", "monthly_charge_segment",
        "has_internet_service", "is_multi_line_user", "contract_type_code"
    ]
    df = df[table_columns]

    total_rows_local = len(df)
    print(f"📌 Local dataset rows: {total_rows_local}")

    # Check for missing values
    missing_cols = ["tenure", "monthlycharges", "totalcharges"]
    missing_summary = {col: df[col].isna().sum() for col in missing_cols}
    print("🔍 Missing values check:")
    for col, missing in missing_summary.items():
        print(f"  - {col}: {missing} missing values")
    if all(v == 0 for v in missing_summary.values()):
        print("✅ No missing values in tenure, MonthlyCharges, TotalCharges")

    # Check for unique row count
    unique_rows = len(df.drop_duplicates())
    if unique_rows == total_rows_local:
        print(f"✅ All rows are unique: {unique_rows} rows")
    else:
        print(f"⚠️ Duplicate rows found: {total_rows_local - unique_rows}")

    # Connect to Supabase
    supabase = get_supabase_client()

    # Fetch all rows from Supabase
    result = supabase.table(table_name).select("*").execute()
    data = result.data

    if not data:
        print("❌ Supabase table is empty")
        return

    df_supabase = pd.DataFrame(data)
    total_rows_db = len(df_supabase)
    print(f"📌 Supabase table rows: {total_rows_db}")

    if total_rows_db == total_rows_local:
        print("✅ Row count matches local dataset")
    else:
        print(f"⚠️ Row count mismatch! Local: {total_rows_local}, Supabase: {total_rows_db}")

    # Validate segments
    tenure_groups = df_supabase['tenure_group'].unique()
    monthly_segments = df_supabase['monthly_charge_segment'].unique()
    print("🔍 Segment validation:")
    print(f"  - Tenure groups: {tenure_groups}")
    print(f"  - Monthly charge segments: {monthly_segments}")

    # Validate contract codes
    contract_codes = set(df_supabase['contract_type_code'].dropna().astype(int).unique())
    valid_codes = {0, 1, 2}
    if contract_codes.issubset(valid_codes):
        print(f"✅ Contract codes valid: {contract_codes}")
    else:
        print(f"⚠️ Invalid contract codes found: {contract_codes - valid_codes}")

    print("🎯 Validation completed ✅")

if __name__ == "__main__":
    local_csv = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "data", "staged", "churn_transformed.csv"
    )
    validate_data(os.path.abspath(local_csv))
