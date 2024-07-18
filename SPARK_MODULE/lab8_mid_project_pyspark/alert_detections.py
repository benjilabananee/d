from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from schemes import enriched_samples_car_schemas

# Initialize Spark session
spark = SparkSession \
    .builder \
    .master("local") \
    .appName('alert_detections23') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

# Read streaming data from Kafka
stream_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "samples-enriched") \
    .option('startingOffsets', 'earliest') \
    .option('failOnDataLoss', 'false')\
    .load() \
    .select(F.col('value').cast(T.StringType()))

# Parse the JSON data in the streaming DataFrame
parsed_df = stream_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), enriched_samples_car_schemas.car_enriched_schema)) \
    .select(F.col('parsed_json.*'))

parsed_df = parsed_df.filter(
    (F.col("speed") > 120) | 
    (F.col("expectedGear") != F.col("gear")) | 
    (F.col("rpm") > 6000)
)

parsed_df.selectExpr("to_json(struct(*)) AS value")\
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "alerttttt") \
    .option('checkpointLocation', 's3a://spark/data/source/cars/checkp') \
    .start()\
    .awaitTermination()


# parsed_df = parsed_df \
# .writeStream \
# .format("console")\
# .outputMode("append")\
# .start()\
# .awaitTermination()
