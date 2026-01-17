import os
import json
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "medical_warehouse")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "12345678")

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cursor = conn.cursor()

# Create raw schema and table if not exists
cursor.execute("""
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.telegram_messages (
    message_id BIGINT PRIMARY KEY,
    channel_name TEXT,
    message_date TIMESTAMP,
    message_text TEXT,
    has_media BOOLEAN,
    image_path TEXT,
    views INT,
    forwards INT
);
""")
conn.commit()

# Path to data lake
data_lake_path = "data/raw/telegram_messages"

# Iterate over JSON files and insert into database
for root, dirs, files in os.walk(data_lake_path):
    for file in files:
        if file.endswith(".json"):
            file_path = os.path.join(root, file)
            with open(file_path, "r", encoding="utf-8") as f:
                messages = json.load(f)
                values = [
                    (
                        m.get("message_id"),
                        m.get("channel_name"),
                        m.get("message_date"),
                        m.get("message_text"),
                        m.get("has_media"),
                        m.get("image_path"),
                        m.get("views"),
                        m.get("forwards")
                    )
                    for m in messages
                ]
                execute_values(
                    cursor,
                    """
                    INSERT INTO raw.telegram_messages
                    (message_id, channel_name, message_date, message_text, has_media, image_path, views, forwards)
                    VALUES %s
                    ON CONFLICT (message_id) DO NOTHING;
                    """,
                    values
                )
conn.commit()
cursor.close()
conn.close()
print("All raw JSON data loaded into PostgreSQL.")
