"""
Module for loading raw JSON data from the data lake into PostgreSQL.

This module reads JSON files from the data lake directory structure and loads
them into the raw schema of the PostgreSQL database.
"""
import os
import json
import logging
from typing import List, Tuple, Optional
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from psycopg2.pool import ThreadedConnectionPool

from config import (
    DatabaseConfig,
    DatabaseSchemaConfig,
    DataPathsConfig
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def get_database_connection():
    """
    Create and return a database connection.
    
    Returns:
        psycopg2.connection: Database connection object.
        
    Raises:
        psycopg2.OperationalError: If connection to database fails.
        psycopg2.Error: For other database connection errors.
    """
    try:
        logger.info(f"Connecting to database: {DatabaseConfig.NAME} at {DatabaseConfig.HOST}")
        conn = psycopg2.connect(**DatabaseConfig.get_connection_params())
        logger.info("Database connection established successfully")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to database: {e}")
        raise
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        raise


def create_schema_and_table(cursor) -> None:
    """
    Create the raw schema and telegram_messages table if they don't exist.
    
    Args:
        cursor: Database cursor object.
        
    Raises:
        psycopg2.Error: If schema or table creation fails.
    """
    try:
        logger.info("Creating raw schema and telegram_messages table if not exists")
        cursor.execute(DatabaseSchemaConfig.CREATE_SCHEMA_QUERY)
        cursor.execute(DatabaseSchemaConfig.CREATE_TABLE_QUERY)
        logger.info("Schema and table creation completed successfully")
    except psycopg2.Error as e:
        logger.error(f"Failed to create schema/table: {e}")
        raise


def parse_message_data(messages: List[dict]) -> List[Tuple]:
    """
    Parse message dictionaries into tuples for database insertion.
    
    Args:
        messages: List of message dictionaries from JSON files.
        
    Returns:
        List of tuples containing message data in database format.
    """
    return [
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


def load_json_file(file_path: str) -> Optional[List[dict]]:
    """
    Load and parse a JSON file containing messages.
    
    Args:
        file_path: Path to the JSON file.
        
    Returns:
        List of message dictionaries, or None if file cannot be read.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            messages = json.load(f)
            logger.debug(f"Loaded {len(messages)} messages from {file_path}")
            return messages
    except FileNotFoundError:
        logger.warning(f"File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in file {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None


def insert_messages_batch(cursor, values: List[Tuple], file_path: str) -> None:
    """
    Insert a batch of messages into the database.
    
    Args:
        cursor: Database cursor object.
        values: List of tuples containing message data.
        file_path: Path to the source file (for logging).
        
    Raises:
        psycopg2.Error: If database insertion fails.
    """
    try:
        execute_values(
            cursor,
            DatabaseSchemaConfig.INSERT_QUERY,
            values
        )
        logger.debug(f"Inserted {len(values)} messages from {file_path}")
    except psycopg2.IntegrityError as e:
        logger.warning(f"Integrity error inserting messages from {file_path}: {e}")
        # Continue processing other files even if one has integrity issues
    except psycopg2.Error as e:
        logger.error(f"Database error inserting messages from {file_path}: {e}")
        raise


def process_data_lake_files(cursor) -> int:
    """
    Process all JSON files in the data lake directory.
    
    Args:
        cursor: Database cursor object.
        
    Returns:
        Number of files processed successfully.
        
    Raises:
        psycopg2.Error: If database operations fail.
    """
    data_lake_path = DataPathsConfig.DATA_LAKE_PATH
    files_processed = 0
    
    if not os.path.exists(data_lake_path):
        logger.warning(f"Data lake path does not exist: {data_lake_path}")
        return files_processed
    
    logger.info(f"Processing JSON files from: {data_lake_path}")
    
    try:
        for root, dirs, files in os.walk(data_lake_path):
            for file in files:
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    
                    # Load messages from JSON file
                    messages = load_json_file(file_path)
                    if messages is None:
                        continue
                    
                    # Parse messages into database format
                    values = parse_message_data(messages)
                    if not values:
                        logger.warning(f"No valid messages found in {file_path}")
                        continue
                    
                    # Insert messages into database
                    try:
                        insert_messages_batch(cursor, values, file_path)
                        files_processed += 1
                    except psycopg2.Error:
                        # Error already logged in insert_messages_batch
                        # Continue processing other files
                        continue
        
        logger.info(f"Successfully processed {files_processed} JSON files")
        return files_processed
    except Exception as e:
        logger.error(f"Unexpected error processing data lake files: {e}")
        raise


def main() -> None:
    """
    Main entry point for loading raw data into PostgreSQL.
    
    Orchestrates database connection, schema creation, and data loading.
    """
    conn = None
    cursor = None
    
    try:
        # Establish database connection
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Create schema and table if needed
        create_schema_and_table(cursor)
        conn.commit()
        
        # Process and load JSON files
        files_processed = process_data_lake_files(cursor)
        conn.commit()
        
        logger.info(f"Successfully loaded raw data. Processed {files_processed} files.")
        print("All raw JSON data loaded into PostgreSQL.")
        
    except psycopg2.Error as e:
        logger.error(f"Database error occurred: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        # Clean up database resources
        if cursor:
            cursor.close()
            logger.debug("Database cursor closed")
        if conn:
            conn.close()
            logger.debug("Database connection closed")


if __name__ == "__main__":
    main()
