from pymongo import MongoClient

# MongoDB connection setup
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
db = client['mydatabase']
print(db.list_collection_names())

print("---------------------------------------------------------------------------------------")

client = MongoClient("mongodb://localhost:27017/")  # Update with your MongoDB URI if needed

# Access the database and collection
db = client["test_db"]
collection = db["test_collection"]

# Define three records to insert
records = [
    {"name": "Alice", "age": 25, "city": "New York"},
    {"name": "Bob", "age": 30, "city": "San Francisco"},
    {"name": "Charlie", "age": 22, "city": "Los Angeles"}
]

# Insert records into the collection
result = collection.insert_many(records)

# Output inserted IDs
print(f"Inserted record IDs: {result.inserted_ids}")

