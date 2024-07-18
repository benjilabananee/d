from pyspark.sql.types import StructType, StructField, IntegerType

car_schema = StructType([
    StructField("car_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("model_id", IntegerType(), True),
    StructField("color_id", IntegerType(), True),
    StructField("row_num", IntegerType(), True)
])