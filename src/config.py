"""
Configuration module for medical-telegram-warehouse.

Centralizes all configuration constants and settings for the application.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any, List

# Load environment variables
load_dotenv()


# Database Configuration
class DatabaseConfig:
    """Database connection configuration."""
    
    HOST: str = os.getenv("DB_HOST", "localhost")
    PORT: int = int(os.getenv("DB_PORT", "5432"))
    NAME: str = os.getenv("DB_NAME", "medical_warehouse")
    USER: str = os.getenv("DB_USER", "postgres")
    PASSWORD: str = os.getenv("DB_PASSWORD", "12345678")
    
    @classmethod
    def get_connection_params(cls) -> Dict[str, Any]:
        """Get database connection parameters as a dictionary."""
        return {
            "host": cls.HOST,
            "port": cls.PORT,
            "dbname": cls.NAME,
            "user": cls.USER,
            "password": cls.PASSWORD
        }


# Telegram API Configuration
class TelegramConfig:
    """Telegram API configuration."""
    
    API_ID: int = int(os.getenv("TELEGRAM_API_ID", "0"))
    API_HASH: str = os.getenv("TELEGRAM_API_HASH", "")
    
    @classmethod
    def validate(cls) -> None:
        """Validate that Telegram API credentials are set."""
        if not cls.API_ID or not cls.API_HASH:
            raise ValueError("Telegram API credentials not found!")


# Data Paths Configuration
class DataPathsConfig:
    """Data paths and directory configuration."""
    
    BASE_DATA_PATH: Path = Path("data/raw")
    MESSAGE_PATH: Path = BASE_DATA_PATH / "telegram_messages"
    IMAGE_PATH: Path = BASE_DATA_PATH / "images"
    DATA_LAKE_PATH: str = "data/raw/telegram_messages"


# Channel Configuration
class ChannelConfig:
    """Channels to scrape configuration."""
    
    CHANNELS: List[str] = [
        "lobelia4cosmetics",
        "CheMed123",
        "tikvahpharma"
    ]


# Schema and Table Configuration
class DatabaseSchemaConfig:
    """Database schema and table definitions."""
    
    RAW_SCHEMA: str = "raw"
    TELEGRAM_MESSAGES_TABLE: str = "telegram_messages"
    
    CREATE_SCHEMA_QUERY: str = f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA};"
    
    CREATE_TABLE_QUERY: str = f"""
    CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.{TELEGRAM_MESSAGES_TABLE} (
        message_id BIGINT PRIMARY KEY,
        channel_name TEXT,
        message_date TIMESTAMP,
        message_text TEXT,
        has_media BOOLEAN,
        image_path TEXT,
        views INT,
        forwards INT
    );
    """
    
    INSERT_QUERY: str = f"""
    INSERT INTO {RAW_SCHEMA}.{TELEGRAM_MESSAGES_TABLE}
    (message_id, channel_name, message_date, message_text, has_media, image_path, views, forwards)
    VALUES %s
    ON CONFLICT (message_id) DO NOTHING;
    """
