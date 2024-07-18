from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark=SparkSession.builder.master("local").appName('ex3_aggregations').getOrCreate()
flights_df = spark.read.parquet('s3a://spark/data/transformed/flights/')
airports_df = spark.read.parquet('s3a://spark/data/source/airports/')
flights_df.printSchema()
airports_df.printSchema()

flights_df.groupBy(F.col('origin_airport_id').alias('airport_id')).agg(F.count(F.lit(1)).alias('number_of_departures')) \
.join(airports_df.select(F.col('airport_id'), F.col('name').alias('airport_name')),
      ['airport_id']) \
.orderBy(F.col('number_of_departures').desc()) \
.show(10, False)