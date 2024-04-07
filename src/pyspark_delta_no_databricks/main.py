# from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()

# from datetime import datetime, date
# from pyspark.sql import Row

# df = spark.createDataFrame([
#     Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
#     Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
#     Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
# ])

# df.show()
# df.printSchema()

# from pyspark.sql import SparkSession

# SparkSession.builder.master("local[*]").getOrCreate().stop()

# spark = SparkSession.builder.remote("sc://localhost").getOrCreate()

# from datetime import datetime, date
# from pyspark.sql import Row

# df = spark.createDataFrame([
#     Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
#     Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
#     Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
# ])
# df.show()

# from pyspark.sql import SparkSession
# from delta import *
# import time
# from pathlib import Path
# import tempfile
# import os
# import shutil

# # SparkSession.builder.master("local[*]").getOrCreate().stop()
# warehouse_dir = tempfile.TemporaryDirectory().name

# print(Path(warehouse_dir).as_uri())

# builder = (
#     SparkSession.builder.appName("MyApp")
#     .master("local[*]")
#     .config("spark.hive.metastore.warehouse.dir", Path(warehouse_dir).as_uri())
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog",
#     )
# )

# spark = configure_spark_with_delta_pip(builder).getOrCreate()

# print(spark.sparkContext)

# from datetime import datetime, date
# from pyspark.sql import Row

# df = spark.createDataFrame(
#     [
#         Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
#         Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
#         Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
#     ]
# )


# df.write.format("delta").saveAsTable("my_table")

# df = spark.sql("SELECT * FROM my_table")
# df.show()


from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row
from pathlib import Path
import tempfile
import os
import shutil
from delta import *

warehouse_dir = tempfile.TemporaryDirectory().name

builder = (
    SparkSession.builder.appName("devcontainer")
    .master("spark://spark:7077")
    # .config("spark.hive.metastore.warehouse.dir", Path(warehouse_dir).as_uri())
    .config("spark.hadoop.fs.s3a.access.key", "HcjjBGuL9gHCA9hUNeiz")
    .config("spark.hadoop.fs.s3a.secret.key", "0GneVJht90WvXjqWUGfwOnQ0RcrA0ZOAUBTKTuRK")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

# https://stackoverflow.com/questions/75472225/java-lang-classnotfoundexception-class-org-apache-hadoop-fs-s3a-s3afilesystem-n
# my_packages = ["org.apache.hadoop:hadoop-aws:3.3.4",
#                "org.apache.hadoop:hadoop-client-runtime:3.3.4",
#                "org.apache.hadoop:hadoop-client-api:3.3.4",
#                "io.delta:delta-contribs_2.12:3.0.0",
#                "io.delta:delta-hive_2.12:3.0.0",
#                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
#                ]

# spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)

# df.write.format("delta").saveAsTable("my_table")

df.write.mode("overwrite").format("delta").save("s3a://delta-lake/my_table")

# df = spark.sql("SELECT * FROM my_table")
# df.show()

# df.show()
