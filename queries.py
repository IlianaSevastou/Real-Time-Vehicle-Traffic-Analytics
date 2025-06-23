from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
db = client["traffic"]

# Query 1: Link with the smallest vehicle count
min_count = db.processed_data.find().sort("vcount", 1).limit(1)
print("Link with smallest vehicle count:", min_count[0]["link"])

# Query 2: Link with the highest average speed
max_speed = db.processed_data.find().sort("vspeed", -1).limit(1)
print("Link with highest average speed:", max_speed[0]["link"])

# Query 3: Longest route
longest_route = db.raw_data.find().sort("x", -1).limit(1)
print("Longest route:", longest_route[0]["link"])
                                                                                       