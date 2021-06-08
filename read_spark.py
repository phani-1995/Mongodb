import pyspark
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/travel.air_travel") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/travel.air_travel") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

# Reading data from mongodb
df = spark.read.format("mongo").load()
df.show()

# Writing the data to database
people = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
   ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])
try:
    #people.write.format("mongo").mode("append").save()
    people.write.format("mongo").mode("append").option("database","people")\
        .option("collection", "contacts").save()
    print("Writing data to mongodb")
except:
    print("Data not written successfully")



# spark = SparkSession \
#     .builder \
#     .appName("Pymongo_example") \
#     .config("'spark.mongodb.input.uri', 'mongodb://localhost:27017/mydb.mycol'") \
#     .getOrCreate()
#
# df = spark.read.format('com.mongodb.spark.sql.DefaultSource').load()
#
# df.createOrReplaceTempView('col')
# Df = spark.sql('select * col')
# Df.show()