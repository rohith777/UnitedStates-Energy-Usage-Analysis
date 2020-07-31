import json
import requests
import pyspark
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType

sc=SparkContext(master="local")
spark = SparkSession.builder \
        .master("local") \
        .appName("Energy Usage") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
SparkSession.builder.config(conf=SparkConf())


if __name__ == "__main__":
    #with open("/Users/narra/Documents/energyUsage/energyUsageData.json", 'r') as j:
        #json_output = json.loads(j.read())
    #json_output = requests.get("/Users/narra/Documents/energyUsage/energyUsageData.json").json()
    new_json_file_df = spark.read.json("/Users/narra/Documents/energyUsage/energyUsageData.json")
    new_json_file_df.printSchema()
    new_json_file_df.show()

    #new_json_file_df.createOrReplaceTempView('my_dictionary')

    new_json_file_df.select(col('category.category_id').alias('Parent_id'),col('category.name').alias('parent'),explode('category.childcategories'),col('col.category_id').alias('id'),col('col.name').alias('name')).show()

    #df.printSchema()
    #df.show()
    