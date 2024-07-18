from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, current_timestamp, rand, lit,row_number,when,col
from schemes import car_schemas
from pyspark.sql import Window


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaProducer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

# Read car data from S3
car_df = spark.read.schema(car_schemas.car_schema).parquet("s3a://spark/data/dims/cars")

car_df.show()
# Read the Parquet file into a DataFrame
