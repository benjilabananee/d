from pyspark.sql import SparkSession
from schemes import car_models_schemas

# Initialize Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("CarModelsParquet") \
    .getOrCreate()

# Define the data
data = [
    (1, "Mazda", "3"),
    (2, "Mazda", "6"),
    (3, "Toyota", "Corolla"),
    (4, "Hyundai", "i20"),
    (5, "Kia", "Sportage"),
    (6, "Kia", "Rio"),
    (7, "Kia", "Picanto")
]

# Create DataFrame
df = spark.createDataFrame(data, car_models_schemas.model_schema)

# Save DataFrame as Parquet in S3
df.write.mode("overwrite").parquet("s3a://spark/data/dims/car_models")

# Stop the Spark session
spark.stop()