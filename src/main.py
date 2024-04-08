from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
# from delta import *


builder = SparkSession.builder.appName("spark_connect_app").remote("sc://spark:15002")
spark = builder.getOrCreate()
# spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create a DataFrame
df = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)

# Write Delta Table to Minio
df.write.mode("overwrite").format("delta").save("s3a://delta-lake/my_table")

# Read Delta Table from Minio
df = spark.read.format("delta").load("s3a://delta-lake/my_table")

# Display Delta Table
df.show()

# DeltaTable.forPath(spark, "s3a://delta-lake/my_table").toDF().show()



