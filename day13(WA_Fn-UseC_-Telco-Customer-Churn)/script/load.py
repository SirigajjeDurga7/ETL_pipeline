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

def create_table_if_not_exists():
    supabase = get_supabase_client()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS public.churn_data (
        id BIGSERIAL PRIMARY KEY,
        tenure INTEGER,
        monthlycharges FLOAT,
        totalcharges FLOAT,
        churn TEXT,
        internetservice TEXT,
        contract TEXT,
        paymentmethod TEXT,
        tenure_group TEXT,
        monthly_charge_segment TEXT,
        has_internet_service INTEGER,
        is_multi_line_user INTEGER,
        contract_type_code INTEGER
    );
    """

    try:
        # Using Supabase RPC to execute raw SQL
        supabase.rpc("execute_sql", {"query": create_table_sql}).execute()
        print("✅ Table 'churn_data' created or already exists")
    except Exception as e:
        print(f"⚠️ Error creating table: {e}")
        print("ℹ️ Ensure the PostgreSQL function 'execute_sql' exists")

def load_to_supabase(staged_path: str, table_name="churn_data"):
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(staged_path)

    print(f"🔍 Reading file at: {staged_path}")

    if not os.path.exists(staged_path):
        print(f"❌ File not found at {staged_path}")
        return

    df = pd.read_csv(staged_path)
    df.columns = df.columns.str.lower()  # lowercase column names

    # Keep only the 12 table columns
    table_columns = [
        "tenure", "monthlycharges", "totalcharges", "churn",
        "internetservice", "contract", "paymentmethod",
        "tenure_group", "monthly_charge_segment",
        "has_internet_service", "is_multi_line_user", "contract_type_code"
    ]
    df = df[table_columns]

    total_rows = len(df)
    supabase = get_supabase_client()

    batch_size = 200
    print(f"📌 Uploading {total_rows} rows in batches of {batch_size}...")

    for start in range(0, total_rows, batch_size):
        batch = df.iloc[start:start + batch_size].copy()
       # Convert NaN → None so Supabase accepts NULL
        batch = batch.where(pd.notnull(batch), None)

# Ensure integer columns are integers
        int_columns = ["has_internet_service", "is_multi_line_user", "contract_type_code"]
        for col in int_columns:
            batch[col] = batch[col].astype(pd.Int64Dtype())  # safely convert to nullable integer


        try:
            supabase.table(table_name).insert(batch.to_dict("records")).execute()
            print(f"✅ Inserted rows {start + 1} ➝ {min(start + batch_size, total_rows)}")
        except Exception as e:
            print(f"❌ ERROR inserting rows {start + 1}-{start + batch_size}: {e}")

    print(f"🎯 Data successfully loaded into table: {table_name}")

if __name__ == "__main__":
    staged_csv = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "data", "staged", "churn_transformed.csv"
    )
    staged_csv = os.path.abspath(staged_csv)

    create_table_if_not_exists()
    load_to_supabase(staged_csv)
