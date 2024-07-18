from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType

model_schema = StructType([
    StructField("modelId",  IntegerType(), True),
    StructField("carBrand",StringType() , True),
     StructField("carModel", StringType(), True),])


