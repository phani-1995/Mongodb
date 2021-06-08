from pymongo import WriteConcern, ReadPreference
import pymongo
from pymongo.read_concern import ReadConcern

client = pymongo.MongoClient("mongodb://localhost:27017/")

wc_majority = WriteConcern("majority", wtimeout=1000)

# Prereq: Create collections.
client.get_database(
    "product", write_concern=wc_majority).product.insert_one({'product': 'prd', 'count': 100})
client.get_database(
    "product", write_concern=wc_majority).sales.insert_one({'product': 'prd', 'count': 4})


# Step 1: Define the callback that specifies the sequence of operations to perform inside the transactions.
def callback(session):
    collection_one = session.client.mydb1.foo
    collection_two = session.client.mydb2.bar

    # Important:: You must pass the session to the operations.
    collection_one.insert_one({'Product': 'prd', 'count': 100}, session=session)
    collection_two.insert_one({'product': 'prd', 'count': 10}, session=session)

    collection_one.update({'count': 100}, {"$set": {'product': 'prd', 'count': 90}})
# Step 2: Start a client session.
with client.start_session() as session:
# Step 3: Use with_transaction to start a transaction, execute the callback, and commit (or abort on error).
	session.with_transaction(
		callback, read_concern=ReadConcern('local'),
		write_concern=wc_majority,
		read_preference=ReadPreference.PRIMARY)




