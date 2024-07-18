from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession \
.builder \
.master("local") \
.config("spark.driver.memory", "4g") \
.appName('ex4_anomalies_detection') \
.getOrCreate()

all_history_window = Window.partitionBy(F.col('Carrier')).orderBy(F.col('flight_date'))

flights_df = spark.read.parquet('s3a://spark/data/transformed/flights/')
flights_df.cache()

avg_delay_df = flights_df\
.withColumn('avg_till_now', F.avg(F.col('arr_delay')).over(all_history_window))

deviation_df = avg_delay_df \
.withColumn('avg_diff_percent', F.abs(F.col('arr_delay') / F.col('avg_till_now')))

outliers_df = deviation_df.where(F.col('avg_diff_percent') > F.lit(3.0))

outliers_df.show()
flights_df.unpersist()
spark.stop()
