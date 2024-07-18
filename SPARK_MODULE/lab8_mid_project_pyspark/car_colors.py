from pyspark.sql import SparkSession
from schemes import colors_schemes  # Import the schema from the schemas.py file

# Initialize Spark session
spark = SparkSession.builder \
    .appName("ColorModelsParquet") \
    .getOrCreate()

# Define the data
color_data = [
    (1, "Black"),
    (2, "Red"),
    (3, "Gray"),
    (4, "White"),
    (5, "Green"),
    (6, "Blue"),
    (7, "Pink")
]

# Create DataFrame using the imported schema
color_df = spark.createDataFrame(color_data, colors_schemes.color_schema)

# Save DataFrame as Parquet in S3
color_df.write.mode("overwrite").parquet("s3a://spark/data/dims/car_colors")

# Stop the Spark session
spark.stop()