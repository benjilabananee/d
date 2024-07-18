from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import rand,row_number
from schemes import car_schemas
from pyspark.sql import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("GenerateCarsParquet") \
    .getOrCreate()


spark = SparkSession.builder \
    .appName("GenerateCarsParquet") \
    .getOrCreate()

# Generate DataFrame with random data for cars
car_df = spark.range(20) \
    .withColumn("car_id", (rand() * 9000000 + 1000000).cast("int")) \
    .withColumn("driver_id", (rand() * 900000000 + 100000000).cast("int")) \
    .withColumn("model_id", (rand() * 7 + 1).cast("int")) \
    .withColumn("color_id", (rand() * 7 + 1).cast("int")) 
            

car_df.show()
# Save DataFrame as Parquet in S3
car_df.write.mode("overwrite").parquet("s3a://spark/data/dims/cars")

# Stop the Spark session
spark.stop()