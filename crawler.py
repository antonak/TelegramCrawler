import signal
import sys
from telethon import TelegramClient
import json
import os
from datetime import datetime
import asyncio
from config import Config

# Directory to save JSON files
DATA_DIR = '/home/antonakd/TelegramCollector/data'
os.makedirs(DATA_DIR, exist_ok=True)

# Get credentials from the Config.py file
api_id = Config['api_id']
api_hash = Config['api_hash']
session_name = Config['username']

monitoring_channels = [
    "PalestineSolidarityBelgium",
    "Eyeonpalestine2",
    "haqqintel",
    "samidounnetwork",
    "resistancechain",
    "PalestinianResistance",
    "PalestineHealth",
    "PalestineUpdates",
    "GazaNow",
    "Palestine2024",
    "FreePalestine2023",
    "StopGazaGenocide",
    "AlQassamBrigades9",
    "palestineresistance",
    "pal_Online9",
    "gazaalanpa",
    "Aqsatvsat"  #spam!!!!
]

# Telegram API rate limit
RATE_LIMIT = 20  # Lower the rate limit for better compliance
DELAY = 1 / RATE_LIMIT
MESSAGES_PER_REQUEST = 100

# Global flag to handle graceful shutdown
stop_signal = False

# Function to handle the Ctrl+C signal
def handle_stop_signal(signum, frame):
    global stop_signal
    stop_signal = True
    print("\nGraceful shutdown initiated...")

# Register signal handler
signal.signal(signal.SIGINT, handle_stop_signal)

async def fetch_messages(client, chat_name):
    try:
        chat_info = await client.get_entity(chat_name)
    except ValueError:
        print(f"Channel '{chat_name}' not found. Skipping...")
        return

    all_messages = []
    offset_id = 0
    while True:
        if stop_signal:
            break

        messages = await client.get_messages(entity=chat_info, limit=MESSAGES_PER_REQUEST, offset_id=offset_id)
        if not messages:
            break

        all_messages.extend(messages)
        offset_id = messages[-1].id
        print(f"Fetched {len(messages)} messages from {chat_name}... Total messages fetched: {len(all_messages)}")

        await save_messages_to_json(messages, chat_name)
        await asyncio.sleep(DELAY)  # Respect the rate limit

    print("_____________________________________________________________________")

async def save_messages_to_json(messages, channel_name):
    today = datetime.now().strftime("%Y-%m-%d")
    file_path = f"{DATA_DIR}/{channel_name}_{today}.json"
    existing_ids = set()
    
    # Check if the file exists; if not, initialize existing_messages
    existing_messages = []
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        with open(file_path, 'r', encoding='utf-8') as file:
            try:
                existing_messages = json.load(file)
                existing_ids = {msg.get('id') for msg in existing_messages if msg.get('id') is not None}
            except json.JSONDecodeError:
                print(f"Error decoding JSON from {file_path}. Starting fresh.")

    for msg in messages:
        msg_dict = serialize_message(msg)
        if msg_dict.get('id') in existing_ids:
            print(f"Message '{msg_dict.get('message')}' (ID: {msg_dict.get('id')}) - Duplicate message ID, skipping...")
        else:
            existing_messages.append(msg_dict)
            print(f"Message '{msg_dict.get('message')}' (ID: {msg_dict.get('id')}) - Saved to {file_path}.")

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(existing_messages, file, ensure_ascii=False, indent=4)

    print(f"Total messages in {file_path}: {len(existing_messages)}")

def serialize_message(msg):
    return {
        'id': msg.id,
        'message': msg.message,
        'timestamp': msg.date.isoformat() if isinstance(msg.date, datetime) else msg.date,
        # Add other fields as necessary
    }

async def main():
    async with TelegramClient(session_name, api_id, api_hash) as client:
        for chat_name in monitoring_channels:
            print(f'Start fetching messages from: {chat_name}')
            await fetch_messages(client, chat_name)
            await asyncio.sleep(DELAY)

# Run the main function
asyncio.run(main())

