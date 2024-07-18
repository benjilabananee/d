from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, rand, current_timestamp
from schemes import car_schemas
import time 

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("KafkaProducer") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2')\
    .getOrCreate()

# Read car data from S3
car_df = spark.read.schema(car_schemas.car_schema).parquet("s3a://spark/data/dims/cars")

car_df.cache()

while True:
    car_event_data = car_df.withColumn("event_id", expr("uuid()")) \
        .withColumn("current_time",current_timestamp())\
        .withColumn("speed", (rand() * 200).cast("int")) \
        .withColumn("rpm", (rand() * 8000).cast("int")) \
        .withColumn("gear", (rand() * 7 + 1).cast("int")) \
        .select("event_id", "current_time", "car_id", "rpm", "gear", "speed") \
        .selectExpr("to_json(struct(*)) as value")

    car_event_data.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "course-kafka:9092") \
        .option("topic", "sensors-sample-last") \
        .option('checkpointLocation', 's3a://spark/data/source/cars/checkpoint') \
        .save()
    
    time.sleep(1)