import pymongo
import pandas as pd
import json

conn = pymongo.MongoClient("mongodb://localhost:27017/")
conn = pymongo.MongoClient()
print("connection made successfully")

# database
db = conn.travel

#Collection
collection = db.air_travel

df = pd.read_csv("airtravel.csv")
data = df.to_dict('records')

data1 = collection.insert_many(data, ordered=False)

cursor = collection.find()
for record in cursor:
    print(record)
#mongodb = MongoDB(dBName='Dataset', collectionName='EnergyConsumption')
