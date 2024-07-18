from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from schemes import event_car

# Initialize Spark session
spark = SparkSession \
    .builder \
    .master("local") \
    .appName('nsrgdsrgdrsgdrgdgdrga') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
    .getOrCreate()

# Read batch data from S3 and cache it
cars_colors = spark.read.parquet('s3a://spark/data/dims/car_colors').cache()
cars = spark.read.parquet("s3a://spark/data/dims/cars").cache()
cars_model = spark.read.parquet("s3a://spark/data/dims/car_models").cache()

# Read streaming data from Kafka
stream_df = spark \
    .readStream \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "sensors-sample-last") \
    .option('startingOffsets', 'earliest') \
    .option('failOnDataLoss', 'false')\
    .load() \
    .select(F.col('value').cast(T.StringType()))

# Parse the JSON data in the streaming DataFrame
parsed_df = stream_df \
    .withColumn('parsed_json', F.from_json(F.col('value'), event_car.car_schema)) \
    .select(F.col('parsed_json.*'))

joined_df = parsed_df \
        .join(cars, on='car_id', how='left') \
        .withColumnRenamed('model_id', 'ModelId')\
        .withColumnRenamed('color_id', 'colorId')\
        .withColumnRenamed('event_id', 'eventId')\
        .withColumnRenamed('car_id', 'carId')\
        .withColumnRenamed('current_time', 'eventTime')\
        .join(cars_model, on='modelId', how='left')\
        .join(cars_colors, on='colorId', how='left')\
        .withColumn("expectedGear", F.round(F.col('speed')/30))\
        .select('eventId', 'carId','driver_id','carBrand','carModel','colorName','speed', 'rpm','gear','expectedGear')

joined_df.selectExpr("to_json(struct(*)) AS value")\
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "samples-enriched") \
    .option('checkpointLocation', 's3a://spark/data/source/cars/checkpoint2') \
    .start()\
    .awaitTermination()

# joined_df = joined_df \
# .writeStream \
# .format("console")\
# .outputMode("append")\
# .start()\
# .awaitTermination()

# query.awaitTermination()