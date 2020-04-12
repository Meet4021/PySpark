# Import package
import pymongo

# Make connection
client = pymongo.MongoClient("mongodb://127.0.0.1:27017/")

import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

conf = pyspark.SparkConf().setMaster("local").setAppName("My First Spark Job").setAll([('spark.driver.memory', '40g'),('spark.executor.memory','50g')])
sc = SparkContext(conf=conf)
sqlC = SQLContext(sc)

# MongoDB Details
mongo_ip = "mongodb://127.0.0.1:27017/MachineLearningData."
# mongo_replica_set_name = "?replicaSet=rs0"

train_df = sqlC.read.format('com.mongodb.spark.sql.DefaultSource').option("uri", mongo_ip + "HouseSalePrice").load()
train_df.createOrReplaceTempView("train_df")
train_df= sqlC.sql("SELECT * FROM train_df")
train_df.show()