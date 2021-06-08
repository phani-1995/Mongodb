from pymongo import WriteConcern, ReadPreference
import pymongo
from pymongo.read_concern import ReadConcern

client = pymongo.MongoClient("mongodb://localhost:27017/")

wc_majority = WriteConcern("majority", wtimeout=1000)

# Prereq: Create collections.
client.get_database(
    "mydb1", write_concern=wc_majority).foo.insert_one({'abc': 'prd', 'count': 100})
client.get_database(
    "mydb2", write_concern=wc_majority).bar.insert_one({'abc': 'prd', 'count': 4})


# Step 1: Define the callback that specifies the sequence of operations to perform inside the transactions.
def callback(session):
    collection_one = session.client.mydb1.foo
    collection_two = session.client.mydb2.bar

    # Important:: You must pass the session to the operations.
    collection_one.insert_one({'abc': 'prd', 'count': 100}, session=session)
    collection_two.insert_one({'xyz': 'prd', 'count': 4}, session=session)
    # collection_one.update({'abc': 'prd', 'count': 96}, session=session)
    collection_one.update({'count': 100}, {"$set": {'abc': 'prd', 'count': 96}}, upsert=False)
# Step 2: Start a client session.
with client.start_session() as session:
# Step 3: Use with_transaction to start a transaction, execute the callback, and commit (or abort on error).
	session.with_transaction(
		callback, read_concern=ReadConcern('local'),
		write_concern=wc_majority,
		read_preference=ReadPreference.PRIMARY)





# # db = client["mydb"]
# # # Collection Name
# # col = db["mycol"]
#
# session = client.start_session()
#
# col_1 = client.get_database("Name")
# col_2 = client.get_database("Sal")
#
# session.start_transaction()
# try:
# 	col_1.insert({"Mark":1})
# 	col_2.insert({"20K":1})
# except:
# 	#operation exception, interrupt transaction
# 	session.abort_transaction()
# else:
# 	session.commit_transaction()
# finally:
# 	session.end_session()