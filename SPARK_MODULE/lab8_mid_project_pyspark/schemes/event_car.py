from pyspark.sql.types import StructType, StructField, IntegerType,StringType,TimestampType

car_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("current_time", TimestampType(), True),
    StructField("car_id", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True),
    StructField("speed", IntegerType(), True) 
])