from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from kafka import KafkaConsumer
import json

uri = ""
# Send a ping to confirm a successful connection
try:
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.toys
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")


consumer = KafkaConsumer('mostsales',bootstrap_servers=[
     'localhost:9092'
     ])

# Parse received data from Kafka
for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    try:
       meme_rec = {'data': record }
       print (record)
       record_id = db.toys.insert_one(meme_rec)
       print("Data inserted with record ids", record_id)
    except:
       print("Could not insert into MongoDB")
