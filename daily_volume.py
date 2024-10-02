import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['Telegram']  # Database name
collection = db['messages']  # Collection name

# Function to retrieve and calculate daily activity
def get_daily_activity():
    # Query to retrieve all messages from the database
    messages = collection.find({})

    daily_volume = defaultdict(int)
    daily_users = defaultdict(set)

    for message in messages:
        msg_date = message.get('date')
        user_id = message.get('from_id')

        if msg_date and user_id:
            msg_day = msg_date.strftime('%Y-%m-%d')
            daily_volume[msg_day] += 1
            daily_users[msg_day].add(user_id)
    
    # Print the number of messages per day
    for day, volume in daily_volume.items():
        print("Messages:")
        print(f'Date: {day}, Messages: {volume}')

    

    # Prepare data for plotting
    data = []
    for day in daily_volume:
        data.append([day, daily_volume[day], len(daily_users[day])])

    # Convert data to a Pandas DataFrame
    df = pd.DataFrame(data, columns=['Date', 'Message Volume', 'Active Users'])
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values('Date', inplace=True)
    
    return df

# Generate daily activity data
df = get_daily_activity()


print("ffffffffffffffffffffffffffffffffffffffffFF")

# Plot the figure
plt.figure(figsize=(10, 6))
plt.plot(df['Date'], df['Message Volume'], label='Message Volume', marker='o')
plt.plot(df['Date'], df['Active Users'], label='Active Users', marker='x')

# Add titles and labels
plt.title('Daily Message Volume and Active Users')
plt.xlabel('Date')
plt.ylabel('Count')
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)

# Save the plot to a file
plt.tight_layout()
plt.savefig('daily_activity_plot.png')  # Saves the figure to a file
plt.close()  # Closes the figure to prevent display
