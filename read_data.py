import pymongo

try:
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    # Database Name
    db = client["mydb"]

    # Collection Name
    col = db["mycol"]

    emp1 = {"name": "Bharatwaj"}
    col.insert(emp1)

    #x = col.find_one()
    x = col.find()
    for record in x:
        print(record)

    print("Data read successfully")
except:
    print("Data was not read successfully")

# emp1 = {"name": "Karthik"}
# col.insert(emp1)