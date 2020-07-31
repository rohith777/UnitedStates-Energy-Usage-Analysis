import json
import requests
import pyspark
from pyspark import SparkContext, SQLContext
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col, explode, posexplode_outer,first,explode_outer, split, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType

sc=SparkContext(master="local")
spark = SparkSession.builder \
        .master("local") \
        .appName("Energy Usage") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
SparkSession.builder.config(conf=SparkConf())

def energyUsageDataset():
        new_json_file_df = spark.read.json("/Users/narra/Documents/energyUsage/Data/energyUsageData.json")
        new_json_file_df.printSchema()
        new_json_file_df.show()
        #colhead = new_json_file_df.select(col('request.series_id')).collect()[0][0]
        df = new_json_file_df.select(explode('series').alias('series'),col('series.name').alias('name'),col('series.units').alias('units'),col('series.f').alias('frequency'),col('series.data').alias('data'))
        df = df.select(col('name'),col('units'),col('frequency'),explode('data').alias('data'))
        df = df.select(col('name'),col('units'),col('frequency'),explode('data').alias('data'))
        df = df.select(col('name'),col('units'),col('frequency'),col("data").getItem(0).alias('date'),col("data").getItem(1).alias('usage'))
        df = df.select(explode('name').alias('name'),col('units'),col('frequency'),col('date'),col('usage')).select(col('name'),explode('units').alias('units'),col('frequency'),col('date'),col('usage')).select(col('name'),col('units'),explode('frequency').alias('frequency'),col('date'),col('usage').alias('usage'))
        df =  df.withColumn("name", regexp_replace('name',"\(i.e., sold to\) ",'').alias('name'))\
                .withColumn("name", regexp_replace('name',"in ",'by ').alias('name'))\
                .withColumn("name", regexp_replace('name',"excluding the sector's share of electrical system energy losses",'').alias('name'))\
                .withColumn("name", regexp_replace('name',"\(delivered to\) ",'').alias('name'))\
                .withColumn("name", regexp_replace('name',"Wood energy",'Wood and waste energy').alias('name'))\
                .withColumn("Category", split(col('name'),',').getItem(0))\
                .withColumn("State", split(col('name'),',').getItem(1))\
                .withColumn("Energy Type", split(col('Category'),'consumed by').getItem(0))\
                .withColumn("Sector", split(col('Category'),'consumed by the').getItem(1))\
                .select(col('Sector'),col('Energy Type'),col('State'),col('date').alias('year'),col('usage').alias('usage_billion_btu'),col('units'),col('frequency'))\
                .groupby('Sector','State','year').pivot('Energy Type').agg(first("usage_billion_btu"))
        df.show()
        #df.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('/Users/narra/Documents/energyUsage/Data/ConvertedData.csv')
        
def energyProductionDataset():
        new_json_file_df = spark.read.json("/Users/narra/Documents/energyUsage/Data/energyProductionData.json")
        new_json_file_df.printSchema()
        new_json_file_df.show()
        df = new_json_file_df.select(explode('series').alias('series'),col('series.name').alias('name'),col('series.units').alias('units'),col('series.f').alias('frequency'),col('series.data').alias('data'))
        df = df.select(col('name'),col('units'),col('frequency'),explode('data').alias('data'))
        df = df.select(col('name'),col('units'),col('frequency'),explode('data').alias('data'))
        df = df.select(col('name'),col('units'),col('frequency'),col("data").getItem(0).alias('date'),col("data").getItem(1).alias('production'))
        df = df.select(explode('name').alias('name'),col('units'),col('frequency'),col('date'),col('production')).select(col('name'),explode('units').alias('units'),col('frequency'),col('date'),col('production')).select(col('name'),col('units'),explode('frequency').alias('frequency'),col('date'),col('production'))
        df =  df.withColumn("name", regexp_replace('name',"\(including lease condensate\)",'').alias('name'))\
                .withColumn("name", regexp_replace('name',"marketed ",'').alias('name'))\
                .withColumn("name", regexp_replace('name',"production",'').alias('name'))\
                .withColumn("Energy Type", split(col('name'),',').getItem(0))\
                .withColumn("State", split(col('name'),',').getItem(1))\
                .select(col('Energy Type'),col('State'),col('date').alias('year'),col('production'),col('units'),col('frequency'))
        df.show()
        df.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('/Users/narra/Documents/energyUsage/Data/ConvertedData.csv')

def populationAndGDPDataset():
        new_json_file_df = spark.read.json("/Users/narra/Documents/energyUsage/Data/populationAndGDPData.json")
        new_json_file_df.printSchema()
        new_json_file_df.show()
        df = new_json_file_df.select(explode('series').alias('series'),col('series.name').alias('name'),col('series.units').alias('units'),col('series.f').alias('frequency'),col('series.data').alias('data'))
        df = df.select(col('name'),col('units'),col('frequency'),explode('data').alias('data'))
        df = df.select(col('name'),col('units'),col('frequency'),explode('data').alias('data'))
        df = df.select(col('name'),col('units'),col('frequency'),col("data").getItem(0).alias('date'),col("data").getItem(1).alias('data'))
        df = df.select(explode('name').alias('name'),col('units'),col('frequency'),col('date'),col('data')).select(col('name'),explode('units').alias('units'),col('frequency'),col('date'),col('data')).select(col('name'),col('units'),explode('frequency').alias('frequency'),col('date'),col('data'))
        df =  df.withColumn("name", regexp_replace('name',"Resident population including Armed Forces",'Population_in_thousands').alias('name'))\
                .withColumn("name", regexp_replace('name',"Current-dollar gross domestic product",'Current_dollar_GDP_MillionUSD').alias('name'))\
                .withColumn("Type", split(col('name'),',').getItem(0))\
                .withColumn("State", split(col('name'),',').getItem(1))\
                .select(col('Type'),col('State'),col('date').alias('year'),col('data'),col('units'),col('frequency'))\
                .groupby('State','year').pivot('Type').agg(first("data"))
        df.show()
        df.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('/Users/narra/Documents/energyUsage/Data/ConvertedData.csv')

if __name__ == "__main__":
        energyUsageDataset()
        #energyProductionDataset()
        #populationAndGDPDataset()
        