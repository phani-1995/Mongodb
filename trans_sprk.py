from pymongo import WriteConcern, ReadPreference
import pymongo
from pymongo.read_concern import ReadConcern

from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/product.product") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/product.sales") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


client = pymongo.MongoClient("mongodb://localhost:27017/")
wc_majority = WriteConcern("majority", wtimeout=1000)

#product = spark.read.format("mongo").option("uri","mongodb://127.0.0.1/product.product").load()
#sales = spark.read.format("mongo").option("uri","mongodb://127.0.0.1/product.sales").load()

def get_mongodb_product():
    return pymongo.MongoClient('mongodb://localhost:27017/product')['product']['product']

def get_mongodb_sales():
    return pymongo.MongoClient('mongodb://localhost:27017/product')['product']['sales']

def create_and_insert(x,y):
    table1 = get_mongodb_product()

    table1.insert_one(x,y)
    # table2.insert_one(x,y)

def create_and_insert1(x,y):
    table2 = get_mongodb_sales()
    table2.insert_one(x, y)


def update_doc():
    table1 = get_mongodb_product()
    table1.update({'count': 120}, {"$set": {'product': 'xyz', 'count': 110}}, upsert=False)

# def callback(session):
#     rdd1 = spark.sparkContext.parallelize([{'product': 'xyz', 'count': 120}])
#     rdd2 = spark.sparkContext.parallelize([{'product': 'xyz', 'count': 10}])
#
#     rdd1.foreach(create_and_insert)
#     rdd2.foreach(create_and_insert1)
#     rdd1.foreach(update_doc)

def callback(session):
    #collection_one = product.rdd.map(lambda x: {x.Col0: x.Col1}).collect()
    collection_one = session.client.product.product
    collection_two = session.client.product.sales
    #collection_two = sales.rdd.map(lambda x: {x.Col0: x.Col1}).collect()

    # Important:: You must pass the session to the operations.
    collection_one.insert_one({'product': 'xyz', 'count': 120}, session=session)
    collection_two.insert_one({'product': 'xyz', 'count': 10}, session=session)
    # collection_one.update({'abc': 'prd', 'count': 96}, session=session)
    collection_one.update({'count': 100}, {"$set": {'product': 'xyz', 'count': 110}}, upsert=False)

with client.start_session() as session:
# Step 3: Use with_transaction to start a transaction, execute the callback, and commit (or abort on error).
	session.with_transaction(
		callback, read_concern=ReadConcern('local'),
		write_concern=wc_majority,
		read_preference=ReadPreference.PRIMARY)

product = spark.read.format("mongo").option("uri","mongodb://127.0.0.1/product.product").load()
product.show()





