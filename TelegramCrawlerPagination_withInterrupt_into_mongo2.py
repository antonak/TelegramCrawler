import signal
import sys
from telethon import TelegramClient
import json
from datetime import datetime
from pymongo import MongoClient
from config import Config
import asyncio

# MongoDB setup
client = MongoClient('localhost', 27017)
db = client['Telegram']  # Database name
collection = db['messages']  # Collection name

# Get credentials from the Config.py file
api_id = Config['api_id']
api_hash = Config['api_hash']
session_name = Config['username']
monitoring_channels = ["pal_Online9"]

# Telegram API rate limit: 30 requests per second
RATE_LIMIT = 30
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

async def fetch_messages(client, chat_name, limit=None):
    """Fetch all messages from a given chat with rate limiting and pagination."""
    chat_info = await client.get_entity(chat_name)
    all_messages = []
    offset_id = 0

    while True:
        if stop_signal:
            break  # Exit if the stop signal is received

        # Fetch messages
        messages = await client.get_messages(entity=chat_info, limit=MESSAGES_PER_REQUEST, offset_id=offset_id)
        
        if not messages:
            break  # Stop when no more messages are returned
        
        all_messages.extend(messages)
        offset_id = messages[-1].id  # Use the last message ID to paginate

        # Print progress
        print(f"Fetched {len(all_messages)} messages so far...")

        # Save messages incrementally to MongoDB
        save_messages_to_mongo(messages)

        # Sleep to avoid hitting the rate limit
        await asyncio.sleep(DELAY)

        # Exit if limit is reached
        if limit and len(all_messages) >= limit:
            break

    return {"messages": all_messages, "channel": chat_info}

def save_messages_to_mongo(messages):
    """Insert messages into MongoDB, avoiding duplicates."""
    for message in messages:
        serialized_message = serialize_message(message)
        
        # Use message ID as the unique identifier to avoid duplicates
        collection.update_one(
            {"id": serialized_message["id"]},  # Check for existing message with the same 'id'
            {"$set": serialized_message},      # Insert or update the message
            upsert=True                        # Ensure no duplicates
        )
        
    print(f"Saved {len(messages)} messages to MongoDB, avoiding duplicates.")

def print_messages_for_date(target_date):
    # Connect to MongoDB
    client = MongoClient('localhost', 27017)
    db = client['Telegram']  # Database name
    collection = db['messages']  # Collection name

    # Convert the target date to a datetime object (start and end of the day)
    start_of_day = datetime.combine(target_date, datetime.min.time())
    end_of_day = datetime.combine(target_date, datetime.max.time())

    # Query to find messages within the date range
    query = {
        'date': {'$gte': start_of_day, '$lt': end_of_day}
    }

    # Retrieve and print the messages
    messages = collection.find(query)
    for message in messages:
        print(f"User: {message.get('from_id')}, Message: {message.get('text')}, Date: {message.get('date')}")
    

def serialize_message(message):
    """Serialize the message, convert dates, and exclude media."""
    msg_dict = message.to_dict()  # Convert the message to a dictionary
    
    # Convert 'date' and 'edit_date' to string formats if they exist
    if isinstance(msg_dict.get('date'), datetime):
        msg_dict['date'] = msg_dict['date'].strftime("%Y_%m_%d_%H_%M_%S")
    if isinstance(msg_dict.get('edit_date'), datetime):
        msg_dict['edit_date'] = msg_dict['edit_date'].strftime("%Y_%m_%d_%H_%M_%S")
    
    # Exclude 'media' field if present
    if 'media' in msg_dict:
        del msg_dict['media']  # Exclude media to keep it simple

    return msg_dict  # MongoDB can directly handle dictionaries

# Main function
async def main():
    async with TelegramClient(session_name, api_id, api_hash) as client:
        for chat_name in monitoring_channels:
            print(f'Start fetching messages from: {chat_name}')
            await fetch_messages(client, chat_name)
    
    # Example usage: Print messages for September 29, 2024
    print_messages_for_date(datetime(2024, 9, 27))
        
# Run the main function
asyncio.run(main())

