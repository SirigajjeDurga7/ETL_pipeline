import pandas as pd
from pathlib import Path
from datetime import datetime

# ----------------------------
# Folder setup
# ----------------------------
BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR / "data" / "raw"
STAGED_DIR = BASE_DIR / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Load latest raw data with flattening
# ----------------------------
def load_latest_file(folder, keyword):
    files = sorted(folder.glob(f"{keyword}_*.json"), reverse=True)
    if not files:
        raise FileNotFoundError(f"No files found for {keyword} in {folder}")
    
    df = pd.read_json(files[0])
    
    # Flatten nested JSON if necessary
    if "deliveries" in df.columns:
        df = pd.json_normalize(df["deliveries"])
    elif "routes" in df.columns:
        df = pd.json_normalize(df["routes"])
    
    return df

# ----------------------------
# Clean Delivery Data
# ----------------------------
def clean_delivery_data(df):
    # Convert timestamps to datetime
    for col in ["dispatch_time", "expected_delivery_time", "actual_delivery_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        else:
            df[col] = pd.NaT

    # Compute delay in minutes first
    df["delay_minutes"] = (df["actual_delivery_time"] - df["expected_delivery_time"]).dt.total_seconds() / 60

    # Remove invalid delays safely
    df = df[df["delay_minutes"].notnull()].copy()

    # Convert weight to kilograms if needed
    df["package_weight_kg"] = df.get("package_weight", 0)

    return df


# ----------------------------
# Delay Classification
# ----------------------------
def classify_delay(df):
    df["delay_class"] = pd.cut(
        df["delay_minutes"],
        bins=[-float("inf"), 0, 60, 180, float("inf")],
        labels=["On-Time", "Slight Delay", "Major Delay", "Critical Delay"]
    )
    return df

# ----------------------------
# Agent Performance Score
# ----------------------------
def compute_agent_score(df):
    df["agent_score"] = pd.cut(
        df["delay_minutes"],
        bins=[-float("inf"), 0, 30, 60, 180, float("inf")],
        labels=[5, 4, 3, 2, 1]
    ).astype(int)
    return df

# ----------------------------
# Merge with Traffic Data
# ----------------------------
def merge_with_traffic(delivery_df, traffic_df):
    # Merge on source city
    delivery_df = delivery_df.merge(
        traffic_df.add_suffix("_source"),
        left_on="source_city",
        right_on="city_source",
        how="left"
    )
    # Merge on destination city
    delivery_df = delivery_df.merge(
        traffic_df.add_suffix("_dest"),
        left_on="destination_city",
        right_on="city_dest",
        how="left"
    )
    return delivery_df

# ----------------------------
# Additional Feature Engineering
# ----------------------------
def feature_engineering(df):
    # Traffic impact score (source + dest)
    df["traffic_impact_score"] = (
        df["traffic_congestion_score_source"].fillna(0) / df["avg_speed_source"].replace(0,1)
        + df["traffic_congestion_score_dest"].fillna(0) / df["avg_speed_dest"].replace(0,1)
    ) * 10

    df["traffic_risk_level"] = pd.cut(
        df["traffic_impact_score"],
        bins=[-float("inf"), 7, 15, float("inf")],
        labels=["Low Risk", "Moderate Risk", "High Risk"]
    )

    # Efficiency score
    df["delivery_efficiency_index"] = (df["package_weight_kg"] / (df["delay_minutes"] + 1)) * df["agent_score"]

    # Predicted delay risk level
    df["predicted_delay_risk_level"] = pd.cut(
        df["delivery_efficiency_index"],
        bins=[-float("inf"), 5, 15, float("inf")],
        labels=["High Risk", "Moderate Risk", "Low Risk"]
    )
    return df

# ----------------------------
# Main Transformation Pipeline
# ----------------------------
def transform_pipeline():
    print("🔄 Starting Transformation Pipeline...")

    # Load latest raw data
    delivery_df = load_latest_file(RAW_DIR, "live_delivery")
    traffic_df = load_latest_file(RAW_DIR, "route_traffic")

    # Clean & process
    delivery_df = clean_delivery_data(delivery_df)
    delivery_df = classify_delay(delivery_df)
    delivery_df = compute_agent_score(delivery_df)
    merged_df = merge_with_traffic(delivery_df, traffic_df)
    final_df = feature_engineering(merged_df)

    # Save transformed dataset
    output_file = STAGED_DIR / f"transformed_delivery_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    final_df.to_csv(output_file, index=False)
    print(f"💾 Transformed dataset saved → {output_file}")
    return output_file

# ----------------------------
# Run transformation if executed directly
# ----------------------------
if __name__ == "__main__":
    transform_pipeline()
