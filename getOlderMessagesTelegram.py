import time
from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import asyncio
from config import Config

# Get credentials from Config.py
api_id = Config['api_id']
api_hash = Config['api_hash']
session_name = Config['username']
message_per_channel = 100  # Retrieve a max of 100 messages per request

monitoring_channels = ["pal_Online9"]  # Add your channels here

# Coroutine to get old messages in batches
async def get_old_messages(chat_name, limit, offset_id):
    async with TelegramClient(session_name, api_id, api_hash) as client:
        chat_info = await client.get_entity(chat_name)
        
        # Retrieve messages with offset_id for pagination
        messages = await client(GetHistoryRequest(
            peer=chat_info,
            limit=limit,
            offset_id=offset_id,
            offset_date=None,
            max_id=0,
            min_id=0,
            add_offset=0,
            hash=0
        ))
        return messages

# Function to introduce delay for rate limiting
def rate_limited_get_messages(chat_name):
    offset_id = 0
    all_messages = []
    
    # Retrieve messages in batches
    while True:
        results = asyncio.run(get_old_messages(chat_name=chat_name, limit=message_per_channel, offset_id=offset_id))
        if not results.messages:
            break  # Stop if there are no more messages
        
        # Append retrieved messages to the total list
        all_messages.extend(results.messages)
        
        # Update offset_id to get older messages in the next request
        offset_id = results.messages[-1].id
        
        # Print retrieved batch count
        print(f"Retrieved {len(results.messages)} messages from {chat_name}")
        
        # Introduce delay to avoid hitting the rate limit
        time.sleep(2)
    
    return all_messages

# Main loop to retrieve messages for each channel
for chat_name in monitoring_channels:
    all_messages = rate_limited_get_messages(chat_name)
    print(f"Total messages retrieved: {len(all_messages)} from {chat_name}")
