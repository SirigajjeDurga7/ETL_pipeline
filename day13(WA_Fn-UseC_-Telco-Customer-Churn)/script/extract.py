import os
import pandas as pd

def extract_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(data_dir, exist_ok=True)

    # Download CSV from URL
    url = "https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv"
    df = pd.read_csv(url)

    # Save to data/raw
    raw_path = os.path.join(data_dir, "churn_raw.csv")

    df.to_csv(raw_path, index=False)

    print(f"✅ Data extracted and saved at: {raw_path}")
    print("Raw path:",raw_path)
    return raw_path

if __name__ == "__main__":
    extract_data()

