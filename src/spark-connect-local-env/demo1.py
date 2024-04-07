from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
import time

builder = SparkSession.builder.appName("devcontainer").master("spark://spark:7077")

spark = builder.getOrCreate()

df = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)

df.show()

time.sleep(100)
