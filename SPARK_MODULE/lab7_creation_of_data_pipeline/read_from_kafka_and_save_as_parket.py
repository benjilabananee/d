from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

json_schema = T.StructType([
    T.StructField('model_id', T.StringType()),
    T.StructField('car_brand', T.StringType()),
    T.StructField('car_model', T.StringType())
])

spark = SparkSession.builder \
    .master("local") \
    .appName('consume_car_models_data') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0') \
    .getOrCreate()

stream_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "cars_model") \
    .option('startingOffsets', 'earliest') \
    .load() \
    .select(F.col('value').cast(T.StringType()))

parsed_df = stream_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
    .select(F.col('parsed_json.*'))

renamed_df = parsed_df.withColumnRenamed('model_id', 'modelId') \
                     .withColumnRenamed('car_brand', 'carBrand') \
                     .withColumnRenamed('car_model', 'carModel')

query = renamed_df \
    .writeStream \
    .format('parquet') \
    .outputMode('append') \
    .option("path", "s3a://spark/data/source/parcket") \
    .option('checkpointLocation', 's3a://spark/checkpoints/ex6/store_result') \
    .start()

query.awaitTermination()