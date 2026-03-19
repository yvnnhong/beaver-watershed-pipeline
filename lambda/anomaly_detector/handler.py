import os
import numpy as np
import psycopg2
from sklearn.ensemble import IsolationForest

# Lambda 4: anomaly_detector
# Reads all records from RDS, z-score normalizes by EPA climate region,
# fits Isolation Forest on 4 water quality features, writes anomaly scores back to RDS.
# Runs after Lambda 3 (processor) in Step Functions.

# EPA climate regions — groups states with similar environmental baselines
# so we don't flag Florida's naturally low DO as anomalous vs Oregon
EPA_REGIONS = {
    "Region1":  ["CT", "ME", "MA", "NH", "RI", "VT"],
    "Region2":  ["NJ", "NY"],
    "Region3":  ["DE", "MD", "PA", "VA", "WV"],
    "Region4":  ["AL", "FL", "GA", "KY", "MS", "NC", "SC", "TN"],
    "Region5":  ["IL", "IN", "MI", "MN", "OH", "WI"],
    "Region6":  ["AR", "LA", "NM", "OK", "TX"],
    "Region7":  ["IA", "KS", "MO", "NE"],
    "Region8":  ["CO", "MT", "ND", "SD", "UT", "WY"],
    "Region9":  ["AZ", "CA", "HI", "NV"],
    "Region10": ["AK", "ID", "OR", "WA"],
}

# invert to state → region lookup
STATE_TO_REGION = {}
for region, states in EPA_REGIONS.items():
    for state in states:
        STATE_TO_REGION[state] = region

def get_region(state_province: str) -> str:
    """Map a state name/abbreviation to EPA region. Default to Region5 if unknown."""
    if not state_province:
        return "Region5"
    # handle full state names by checking if any state code appears in the string
    s = state_province.upper().strip()
    for code, region in STATE_TO_REGION.items():
        if code in s:
            return region
    return "Region5"  # default for unknown

def zscore_normalize(features: np.ndarray, region_labels: list) -> np.ndarray:
    """
    Z-score normalize each feature within EPA climate region.
    For each feature x in region r: z = (x - mean_r) / std_r
    This ensures Florida's naturally low DO isn't flagged vs Oregon's higher DO.
    """
    normalized = features.copy().astype(float)
    unique_regions = set(region_labels)

    for region in unique_regions:
        mask = np.array([r == region for r in region_labels])
        region_data = features[mask]
        for col in range(features.shape[1]):
            col_data = region_data[:, col]
            mean = np.mean(col_data)
            std = np.std(col_data)
            if std > 0:
                normalized[mask, col] = (features[mask, col] - mean) / std
            else:
                normalized[mask, col] = 0.0  # all same value, no variance

    return normalized

def lambda_handler(event: dict, context: object) -> dict:
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname="postgres",
        port=5432
    )
    cursor = conn.cursor()

    # fetch all records with complete water quality data
    cursor.execute("""
        SELECT id, avg_dissolved_oxygen, avg_water_temp, avg_ph, avg_turbidity, state_province
        FROM beaver_water_joined
        WHERE avg_dissolved_oxygen IS NOT NULL
          AND avg_water_temp IS NOT NULL
          AND avg_ph IS NOT NULL
          AND avg_turbidity IS NOT NULL
    """)
    rows = cursor.fetchall()
    print(f"Fetched {len(rows)} records with complete water quality data")

    if len(rows) == 0:
        print("No records to score!")
        return {"statusCode": 200, "recordsScored": 0, "anomaliesDetected": 0}

    ids = [r[0] for r in rows]
    features = np.array([[r[1], r[2], r[3], r[4]] for r in rows])
    region_labels = [get_region(r[5]) for r in rows]

    # z-score normalize within EPA climate regions
    print("Normalizing by EPA climate region...")
    normalized = zscore_normalize(features, region_labels)

    # fit Isolation Forest on normalized features
    print("Fitting Isolation Forest...")
    clf = IsolationForest(contamination=0.05, random_state=42, n_estimators=100)
    scores = clf.fit_predict(normalized)  # 1 = normal, -1 = anomaly

    # write scores back to RDS
    print("Writing anomaly scores to RDS...")
    update_data = [(int(score), id_) for score, id_ in zip(scores, ids)]
    cursor.executemany(
        "UPDATE beaver_water_joined SET anomaly_score = %s WHERE id = %s",
        update_data
    )

    n_anomalies = int(sum(1 for s in scores if s == -1))
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Scored {len(ids)} records. Anomalies: {n_anomalies} ({n_anomalies/len(ids)*100:.1f}%)")
    return {
        "statusCode": 200,
        "recordsScored": len(ids),
        "anomaliesDetected": n_anomalies
    }