# transform.py

import os
import pandas as pd

def transform_data(raw_path):
    # Create staged folder path
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
    df = pd.read_csv(raw_path)

    df["TotalCharges"] = pd.to_numeric(df["TotalCharges"], errors="coerce")

    numeric_cols = ["tenure", "MonthlyCharges", "TotalCharges"]
    for col in numeric_cols:
        df[col] = df[col].fillna(df[col].median())

    categorical_cols = df.columns[df.dtypes == "object"]
    df[categorical_cols] = df[categorical_cols].fillna("Unknown")


    df["tenure_group"] = pd.cut(
        df["tenure"],
        bins=[0, 12, 36, 60, float("inf")],
        labels=["New", "Regular", "Loyal", "Champion"],
        right=True
    )

    df["monthly_charge_segment"] = pd.cut(
        df["MonthlyCharges"],
        bins=[0, 30, 70, float("inf")],
        labels=["Low", "Medium", "High"],
        right=False
    )

    df["has_internet_service"] = df["InternetService"].map({
        "DSL": 1,
        "Fiber optic": 1,
        "No": 0
    })

    df["is_multi_line_user"] = df["MultipleLines"].map({"Yes": 1}).fillna(0)

    df["contract_type_code"] = df["Contract"].map({
        "Month-to-month": 0,
        "One year": 1,
        "Two year": 2
    })


    df = df.drop(columns=["customerID", "gender"], errors="ignore")


    staged_path = os.path.join(staged_dir, "churn_transformed.csv")
    df.to_csv(staged_path, index=False)

    print(f"🚀 Data transformed and saved at: {staged_path}")
    return staged_path


# Run directly
if __name__ == "__main__":
    from extract import extract_data  # Import from extract
    raw_path = extract_data()         # Get raw dataset path
    transform_data(raw_path)          # Transform dataset
