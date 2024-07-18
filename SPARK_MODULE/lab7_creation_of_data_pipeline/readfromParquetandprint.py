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
    .appName('readfromparquet') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()


# Define the schema for the streaming data

schema = StructType([
    StructField("modelId",  StringType(), True),
    StructField("carBrand", StringType(), True),
     StructField("carModel", StringType(), True),])


