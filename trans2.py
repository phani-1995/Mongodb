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

def get_mongodb_product():
    return pymongo.MongoClient('mongodb://localhost:27017/product')['product']['product']

def get_mongodb_sales():
    return pymongo.MongoClient('mongodb://localhost:27017/product')['product']['sales']

def create_and_insert(x):
    table1 = get_mongodb_product()
    table1.insert_one(x)

def create_and_insert1(x):
    table2 = get_mongodb_sales()
    table2.insert_one(x)

def update_doc(x):
    table1 = get_mongodb_product()
    table1.update_one(x)

def callback(session):
    col1 = session.client.product.product
    col2 = get_mongodb_sales()
    rdd1 = spark.sparkContext.parallelize([{'product': 'car', 'count': 150}])
    rdd1.foreach(create_and_insert)
    rdd2 = spark.sparkContext.parallelize([{'product': 'car', 'count': 45}])
    rdd2.foreach(create_and_insert1)

    col1.update({'count': 150}, {"$set": {'product': 'car', 'count': 105}}, upsert=False)

    #rdd1.foreach(update_doc)

with client.start_session() as session:
# Step 3: Use with_transaction to start a transaction, execute the callback, and commit (or abort on error).
	session.with_transaction(
		callback, read_concern=ReadConcern('local'),
		write_concern=wc_majority,
		read_preference=ReadPreference.PRIMARY)

product = spark.read.format("mongo").option("uri","mongodb://127.0.0.1/product.product").load()
product.show()

product = spark.read.format("mongo").option("uri","mongodb://127.0.0.1/product.sales").load()
product.show()