import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto

from config import TelegramConfig, ChannelConfig, DataPathsConfig

# Validate Telegram API credentials
TelegramConfig.validate()

# Configure logging
LOG_FILE = f"logs/scraper_{datetime.now().strftime('%Y_%m_%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Helper function to save JSON messages

def save_messages(messages: List[Dict[str, Any]], channel_name: str) -> None:
    """
    Save messages to a JSON file in the data lake directory structure.
    
    Args:
        messages: List of message dictionaries to save.
        channel_name: Name of the channel being scraped.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    output_dir = DataPathsConfig.MESSAGE_PATH / today
    output_dir.mkdir(parents = True, exist_ok = True)

    file_path = output_dir / f"{channel_name}.json"

    with open(file_path, "w", encoding = "utf-8") as f:
        json.dump(messages, f, ensure_ascii = False, indent = 2)

    logging.info(f"Saved {len(messages)} messages for {channel_name}")

# Main scraping function

async def scrape_channel(client: TelegramClient, channel_name: str) -> None:
    """
    Scrape messages from a Telegram channel and save them to the data lake.
    
    Args:
        client: TelegramClient instance for API access.
        channel_name: Name of the channel to scrape.
    """
    logging.info(f"Scraping channel: {channel_name}")
    messages_data = []

    channel_image_dir = DataPathsConfig.IMAGE_PATH / channel_name
    channel_image_dir.mkdir(parents  =True, exist_ok = True)

    async for message in client.iter_messages(channel_name, limit = 1000):
        msg = {
            "message_id" :message.id,
            "channel_name" : channel_name,
            "message_date" : message.date.isoformat() if message.date else None,
            "message_text" : message.text,
            "views" : message.views,
            "forwards" : message.forwards,
            "has_media" : message.media is not None,
            "image_path" : None

        }

        # Download image if exists
        if isinstance(message.media, MessageMediaPhoto):
            image_file = channel_image_dir / f"{message.id}.jpg"
            await client.download_media(message.media, image_file)
            msg["image_path"] = str(image_file)

        messages_data.append(msg)

    save_messages(messages_data, channel_name)


# Main entry point

async def main() -> None:
    """
    Main entry point for the Telegram scraper.
    
    Iterates through configured channels and scrapes their messages.
    """
    async with TelegramClient("telegram_session", TelegramConfig.API_ID, TelegramConfig.API_HASH) as client:
        for channel in ChannelConfig.CHANNELS:
            try: 
                await scrape_channel(client, channel)
            except Exception as e:
                logging.error(f"Failed to scrape {channel} : {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())