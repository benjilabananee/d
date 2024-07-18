from pyspark.sql.types import StructType, StructField, IntegerType,StringType,TimestampType,DoubleType

car_enriched_schema = StructType([
    StructField("eventId", StringType(), True),
    StructField("carId", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("carBrand", StringType(), True),
    StructField("carModel", StringType(), True),
    StructField("colorName", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
     StructField("gear", IntegerType(), True),
     StructField("expectedGear", DoubleType(), True)
])
