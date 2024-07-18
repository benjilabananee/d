from pyspark.sql.types import StructType, StructField, StringType, IntegerType

color_schema = StructType([
    StructField("colorId", IntegerType(), True),
    StructField("colorName", StringType(), True)
])