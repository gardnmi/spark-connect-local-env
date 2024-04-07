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
    .remote("sc://spark:15002")
    # .config("spark.hive.metastore.warehouse.dir", Path(warehouse_dir).as_uri())
    # .config("spark.hadoop.fs.s3a.access.key", "d4pJ9Bknx2CCvWWCpJ1P")
    # .config("spark.hadoop.fs.s3a.secret.key", "cQnS4lcUbyDvLjSxm3xjbYjmOlWb5m6YTevb9FQs")
    # .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    # .config("spark.hadoop.fs.s3a.path.style.access", "true")
    # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # .config(
    #     "spark.sql.catalog.spark_catalog",
    #     "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    # )
)

# https://stackoverflow.com/questions/75472225/java-lang-classnotfoundexception-class-org-apache-hadoop-fs-s3a-s3afilesystem-n
# my_packages = ["org.apache.hadoop:hadoop-aws:3.3.4",
#                "org.apache.hadoop:hadoop-client-runtime:3.3.4",
#                "org.apache.hadoop:hadoop-client-api:3.3.4",
#                "io.delta:delta-contribs_2.12:3.0.0",
#                "io.delta:delta-hive_2.12:3.0.0",
#                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
#                ]


spark = builder.getOrCreate()

# spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()

# spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.createDataFrame(
    [
        Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
        Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
        Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
    ]
)

df.write.mode("overwrite").format("delta").save("s3a://delta-lake/my_table")