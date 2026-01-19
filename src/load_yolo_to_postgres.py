# src/load_yolo_to_postgres.py

import csv
import psycopg2
from pathlib import Path

# -----------------------------
# Configuration
# -----------------------------
CSV_FILE = Path("data/processed/yolo_detections.csv")

DB_CONFIG = {
    "dbname": "medical_warehouse",
    "user": "postgres",
    "password": "12345678",
    "host": "localhost",
    "port": 5432,
}

# -----------------------------
# Load CSV into PostgreSQL
# -----------------------------
def load_yolo_csv():
    if not CSV_FILE.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE}")

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    with open(CSV_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            cur.execute(
                """
                INSERT INTO raw.yolo_detections (
                    image_name,
                    detected_objects,
                    image_category,
                    confidence_score
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (image_name) DO NOTHING
                """,
                (
                    row["image_name"],
                    row["detected_objects"],
                    row["image_category"],
                    row["confidence_score"],
                )
            )

    conn.commit()
    cur.close()
    conn.close()

    print("✅ YOLO CSV loaded into raw.yolo_detections")

# -----------------------------
# Entry point
# -----------------------------
if __name__ == "__main__":
    load_yolo_csv()
