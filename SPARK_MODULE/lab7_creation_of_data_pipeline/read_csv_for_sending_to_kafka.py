from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

# Initialize Spark Session for Structured Streaming

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('S3_cars_to_kafka') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

schema = StructType([
    StructField("model_id", StringType(), True),
    StructField("car_brand", StringType(), True),
    StructField("car_model", StringType(), True),
    # Add other fields based on your CSV structure
])

df = spark.readStream \
    .format("csv") \
    .schema(schema)\
    .option("header", "true") \
    .load(f"s3a://spark/data/source/cars")

df_json = df.selectExpr("to_json(struct(*)) AS value")


df.selectExpr("to_json(struct(*)) AS value")\
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "cars_model") \
    .option('checkpointLocation', 's3a://spark/data/source/cars/checkpoint') \
    .start()\
    .awaitTermination()



