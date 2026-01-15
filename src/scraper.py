import os
import json
import logging
from datetime import datetime
from pathlib import Path

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv
from tqdm import tqdm
#load env't variables
load_dotenv()
API_ID = int(os.getenv("TELEGRAM_API_ID"))
API_HASH = os.getenv("TELEGRAM_API_HASH")

if not API_ID or not API_HASH:
    raise ValueError("Telegram API credentials not found!.")
#configure logging
LOG_FILE = f"logs/scraper_{datetime.now().strftime('%Y_%m_%d')}.log"

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    handlers = [
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

#define the channel to scrape

CHANNELS  =[
    "lobelia4cosmetics",
    "CheMed123",
    "tikvahpharma"
]
#create data lake path
BASE_DATA_PATH = Path("data/raw")
MESSAGE_PATH = BASE_DATA_PATH / "telegram_messages"
IMAGE_PATH = BASE_DATA_PATH / "images"

# a helper function : to save json

def save_messages(messages, channel_name):
    today = datetime.now().strftime("%Y-%m-%d")
    output_dir = MESSAGE_PATH / today
    output_dir.mkdir(parents = True, exist_ok = True)

    file_path = output_dir / f"{channel_name}.json"

    with open(file_path, "w", encoding = "utf-8") as f:
        json.dump(messages, f, ensure_ascii = False, indent = 2)

    logging.info(f"Saved {len(messages)} messages for {channel_name}")

#main scraping function

async def scrape_channel(client , channel_name):
    logging.info(f"Scraping channel : {channel_name}")
    messages_data = []

    channel_image_dir = IMAGE_PATH / channel_name
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


# main entry point 

async def main():
    async with TelegramClient("telegram_session", API_ID, API_HASH) as client:
        for channel in CHANNELS:
            try: 
                await scrape_channel(client, channel)
            except Exception as e:
                logging.error(f"Failed to scrape {channel} : {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())