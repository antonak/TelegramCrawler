
'''
Interrupt Handling: Catch keyboard interruptions (Ctrl+C) and use Python's signal handling to enable graceful quitting.
Regular Saving: After each batch of messages, save the messages to the file rather than waiting for them to all be retrieved.
Advancement Printing: Print the progress (the total number of messages retrieved) continuously.



---------


Important Updates: Kindly Pause:

In order to capture the Ctrl+C interrupt (SIGINT), the handle_stop_signal function is registered using signal.signal.
The loop will end with the current fetch when you hit Ctrl+C, making sure no data is lost.
Gradual Savings:

After every batch is fetched, messages are stored to a file so that in the event of an interruption,

'''


import signal
import sys
from telethon import TelegramClient
import json
from datetime import datetime
from config import Config  # Import the API id, hash from here
import asyncio

# Get credentials from the Config.py file
api_id = Config['api_id']
api_hash = Config['api_hash']
session_name = Config['username']
monitoring_channels = ["pal_Online9"]

# Telegram API rate limit: 30 requests per second
RATE_LIMIT = 30  # requests per second
DELAY = 1 / RATE_LIMIT  # Delay between requests in seconds
MESSAGES_PER_REQUEST = 100  # Maximum messages per request

# Global flag to handle graceful shutdown
stop_signal = False

# Function to handle the Ctrl+C signal and set the stop flag
def handle_stop_signal(signum, frame):
    global stop_signal
    stop_signal = True
    print("\nGraceful shutdown initiated...")

# Register signal handler
signal.signal(signal.SIGINT, handle_stop_signal)

async def fetch_messages(client, chat_name, limit=None):
    """Fetch all messages from a given chat with rate limiting, pagination, and progress."""
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

        # Sleep to avoid hitting the rate limit
        await asyncio.sleep(DELAY)

        # Exit if limit is reached
        if limit and len(all_messages) >= limit:
            break

        # Save messages incrementally
        save_messages(all_messages, chat_name)

    return {"messages": all_messages, "channel": chat_info}

def save_messages(messages, chat_name):
    """Save the fetched messages to a file."""
    filename = get_filename(chat_name)
    with open(filename, 'a') as f:
        for msg in messages:
            f.write(f'{serialize_message(msg)}\n')

def get_date():
    current_date_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    return current_date_time

def get_filename(chat_name):
    return f'{chat_name}__{get_date()}.txt'


'''
def serialize_message(message):
    """Serialize the message, convert dates, and exclude media."""
    msg_dict = message.to_dict()
    if msg_dict['date']:
        msg_dict['date'] = msg_dict['date'].strftime("%Y_%m_%d_%H_%M_%S")
    if msg_dict.get('edit_date'):
        msg_dict['edit_date'] = msg_dict['edit_date'].strftime("%Y_%m_%d_%H_%M_%S")
    if 'media' in msg_dict:
        del msg_dict['media']  # Exclude media to keep it simple
    return json.dumps(msg_dict)
'''

def serialize_message(message):
    """Serialize the message, convert dates, and exclude media."""
    msg_dict = message.to_dict()
    
    # Convert 'date' and 'edit_date' to string formats if they exist
    if isinstance(msg_dict.get('date'), datetime):
        msg_dict['date'] = msg_dict['date'].strftime("%Y_%m_%d_%H_%M_%S")
    
    if isinstance(msg_dict.get('edit_date'), datetime):
        msg_dict['edit_date'] = msg_dict['edit_date'].strftime("%Y_%m_%d_%H_%M_%S")
    
    # Exclude 'media' field if present
    if 'media' in msg_dict:
        del msg_dict['media']  # Exclude media to keep it simple
    
    # Return the JSON representation of the message
    return json.dumps(msg_dict)


# Main function
async def main():
    async with TelegramClient(session_name, api_id, api_hash) as client:
        for chat_name in monitoring_channels:
            print(f'Start fetching messages from: {chat_name}')
            await fetch_messages(client, chat_name)

# Run the main function
asyncio.run(main())
