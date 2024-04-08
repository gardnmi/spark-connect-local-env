# Spark Connect with Minio and Delta Lake

## Summary

This post will walk you through setting up a pyspark application that connects to a remote spark cluster using Spark Connect.  

You can find the working code in the [GitHub Repo](https://github.com/gardnmi/spark-connect-local-env)

## Motivation

With the looming relase of Spark 4.0 it appears Spark Connect will be the main way to communicate with remote spark clusters. Currently the documentation is a bit sparse and there aren't many examples outside of some toy examples using local mode.  I hope this post will help others to get started with Spark Connect and also provide more discussion around the topic.

## Prerequisites

* Docker
* Visual Studio Code

## Setting up the Environment

Our environment configuration is contained within a docker-compose file which consists following services:
1. pyspark application
2. Minio
3. Spark Master
4. Spark Worker


### Pyspark Service

The first step in this project is to build the pyspark application that will connect to the remote spark cluster.  We will use Visual Studio Code with the Dev Containers extension to create a containerized environment for the application.

Steps to create the application:
1. Create an empty project folder on your local machine.
2. Open the project folder in Visual Studio Code
3. Create a folder called `.devcontainer`
4. Inside the .devcontainer folder create a file called `devcontainer.json` and add the following code:

```json
{
	"name": "Python 3",
	"dockerComposeFile": "docker-compose.yml",
	"service": "devcontainer",
	"workspaceFolder": "/workspaces/",
	"shutdownAction": "stopCompose",
	"postCreateCommand": "bash .devcontainer/conf/.postCreateCommand.sh"
}
```

* Note: For more information on the devcontainer.json file see the [Visual Studio Code Documentation](https://code.visualstudio.com/docs/remote/containers#_devcontainerjson-reference)

5. Inside the .devcontainer folder create a file called `docker-compose.yml` and add the following code to the docker-compose.yml file:

```yaml
version: '3.8'

services:
  devcontainer:
    image: mcr.microsoft.com/devcontainers/python:1-3.11-bullseye
    volumes:
      - ..:/workspaces:cached
    command: sleep infinity    
```
6. Create a folder called `src` in the root of the project and create a file called `main.py` and add the following code:

```python
from pyspark.sql import SparkSession
from datetime import datetime, date
from pyspark.sql import Row


builder = SparkSession.builder.appName("spark_connect_app").remote("sc://spark:15002")
spark = builder.getOrCreate()

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
```

7. Create a file called `requirements.txt` in the root of the project and add the following code:

```
pyspark[connect]==3.5.1
delta-spark~=3.1.0
```
  * Note: The version of pyspark[connect] should match the version of spark you are using.  In this example we are using Spark 3.5.1
  * Note: Use the [Compatablity Matrix for Delta Lake](https://docs.delta.io/latest/releases.html) to choose the correct version of delta-spark


8. Create a file called `.postCreateCommand.sh` in the .devcontainer/conf folder and add the following code:

```bash
# !/bin/bash
sudo apt-get update && sudo apt-get -y upgrade

# Install Python Package
pip install --upgrade pip
pip install -r requirements.txt
```

* Note: The .postCreateCommand.sh file is used to install the required python packages when our application container is started.


### Minio Services
Minio is an open source object storage server that is compatible with Amazon S3.  We will use Minio to store the delta table created by our pyspark application.

Steps to setup Minio:
1. Open .devcontainer/docker-compose.yml and append the following code:

```yaml
# Existing Code Above

  minio:
    image: minio/minio:latest
    volumes:
      - minio-data:/data
    environment:
      - MINIO_ROOT_USER=minioadmin
      - MINIO_ROOT_PASSWORD=minioadmin
    command: server /data --console-address ":9001"
    ports:    
      - '9000:9000'
      - '9001:9001'

  minio_mc:
    image: minio/mc
    environment:
      - MINIO_ACCESS_KEY=minioadmin
      - MINIO_SECRET_KEY=minioadmin
    volumes:
      - ./conf/minio_mc-entrypoint.sh:/usr/bin/entrypoint.sh
    entrypoint: /bin/sh /usr/bin/entrypoint.sh
    depends_on:
      - minio
```

2. Create a folder called `conf` in the .devcontainer folder and create a file called `minio_mc-entrypoint.sh` and add the following code:

```bash
#/bin/bash  
    
/usr/bin/mc config host add local http://minio:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY}  
/usr/bin/mc mb local/delta-lake
exit 0
```

* Note: The minio service is setup to use the default credentials of minioadmin/minioadmin.  The minio_mc service is used to pre-create a bucket called delta-lake when we start the minio service.

### Spark Services
Our spark cluster will consist of a spark master and a spark worker.  We will use the bitnami/spark image as a base for the spark services.

Steps to setup the spark cluster:
1. Open `.devcontainer/docker-compose.yml` and append the following code:

```yaml
# Existing Code Above

  spark:
    build:
      context: .
      dockerfile: Dockerfile.spark              
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_USER=spark
    ports:
      - '4040:4040' # Spark UI           
      - '8080:8080' # Spark Master
      - '7077:7077'
    volumes:
      - ./conf/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf

  spark-worker:
    image: docker.io/bitnami/spark:3.5.1
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark:7077
      - SPARK_WORKER_MEMORY=1G
      - SPARK_WORKER_CORES=1
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_USER=spark 
    volumes:
      - ./conf/spark-defaults.conf:/opt/bitnami/spark/conf/spark-defaults.conf        
  
volumes:
  minio-data:
```

* Note: The default bitnami/spark image does not start the spark connect server on startup and is also missing some jar files that are needed to use delta lake.  We will create a custom Dockerfile that extends the bitnami/spark image and adds the necessary jar files and starts the spark connect server.

2. Create a file called `Dockerfile.spark` in the .devcontainer folder and add the following code:

```Dockerfile
FROM docker.io/bitnami/spark:3.5.1

# modifying the bitnami image to start spark-connect in master
COPY ./conf/run-spark.sh /opt/bitnami/scripts/spark/run-spark.sh
CMD [ "/opt/bitnami/scripts/spark/run-spark.sh"  ]

# adding additional jars for delta lake
# https://github.com/bitnami/containers/blob/main/bitnami/spark/README.md#installing-additional-jars
USER root
RUN install_packages curl
USER 1001
RUN curl https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.1.0/delta-spark_2.12-3.1.0.jar --output /opt/bitnami/spark/jars/delta-spark_2.12-3.1.0.jar
RUN curl https://repo1.maven.org/maven2/io/delta/delta-storage/3.1.0/delta-storage-3.1.0.jar --output /opt/bitnami/spark/jars/delta-storage-3.1.0.jar
```

* Note: To start the spark connect server we need to run the `start-connect-server.sh` script with the argument `--packages org.apache.spark:spark-connect_2.12:3.5.1` on our spark master.

  To achieve this, we modified the CMD command in the dockerfile to run a slightly modified version of the run.sh script that the base image uses to start spark.

3. Create a file in .devcontainer/conf called `run-spark.sh` and add the following code:

```bash
#!/bin/bash
# Copyright VMware, Inc.
# SPDX-License-Identifier: APACHE-2.0

# shellcheck disable=SC1091

set -o errexit
set -o nounset
set -o pipefail
#set -o xtrace

# Load libraries
. /opt/bitnami/scripts/libspark.sh
. /opt/bitnami/scripts/libos.sh

# Load Spark environment settings
. /opt/bitnami/scripts/spark-env.sh

if [ "$SPARK_MODE" == "master" ]; then
    # Master constants
    EXEC=$(command -v start-master.sh)
    ARGS=()
    EXEC_CONNECT=$(command -v start-connect-server.sh) 
    info "** Starting Spark in master mode **"
else
    # Worker constants
    EXEC=$(command -v start-worker.sh)
    ARGS=("$SPARK_MASTER_URL")
    info "** Starting Spark in worker mode **"
fi
if am_i_root; then
    # exec_as_user 
    if [ "$SPARK_MODE" == "master" ]; then
        "$SPARK_DAEMON_USER" "$EXEC" "${ARGS[@]-}" &
        "$SPARK_DAEMON_USER" "$EXEC_CONNECT" --packages org.apache.spark:spark-connect_2.12:3.5.1
    else
        exec "$SPARK_DAEMON_USER" "$EXEC" "${ARGS[@]-}"
    fi
else
    # exec 
    if [ "$SPARK_MODE" == "master" ]; then
        "$EXEC" "${ARGS[@]-}" &
        "$EXEC_CONNECT" --packages org.apache.spark:spark-connect_2.12:3.5.1
    else
        exec "$EXEC" "${ARGS[@]-}"
    fi
fi
```

The final step is to add configuration to our spark cluster to allow it to connect to minio and use delta lake.
Note: This is documented in the bitnami/spark image [here](https://github.com/bitnami/containers/blob/main/bitnami/spark/README.md#mount-a-custom-configuration-file)

4. Create a file called spark-defaults.conf in the .devcontainer/conf folder and add the following code to the spark-defaults.conf file:

```properties
spark.sql.extensions             io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog  org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.access.key minioadmin
spark.hadoop.fs.s3a.secret.key minioadmin
spark.hadoop.fs.s3a.endpoint http://minio:9000
```

* Note: The spark-defaults.conf file is used to configure the spark cluster to use the delta lake and minio.

* Note: More information on modifying the bitnami/spark image can be found [here](https://github.com/bitnami/containers/blob/main/bitnami/spark/README.md#mount-a-custom-configuration-file)

## Running the Application

To run the application:
1. Open the project in Visual Studio Code if it is not already open
2. Open the command palette (Ctrl+Shift+P) and run the command `Remote-Containers: Reopen in Container`
3. Open a terminal in Visual Studio Code and run the following command:

```console
python src/main.py
```

The application will create a dataframe and write a delta table to Minio.  It will then read the delta table from Minio and display the contents of the table.

Output:
```console
+---+---+-------+----------+-------------------+
|  4|5.0|string3|2000-03-01|2000-01-03 12:00:00|
|  1|2.0|string1|2000-01-01|2000-01-01 12:00:00|
|  2|3.0|string2|2000-02-01|2000-01-02 12:00:00|
+---+---+-------+----------+-------------------+
```


To view the underlying files of the delta table you can use the Minio web console.  

The Minio web console can be accessed at [localhost:9001](http://localhost:9001/).  

* Note: Use the default credentials `minioadmin/minioadmin` to login.

To view the spark connect job open the Spark UI at [localhost:4040](http://localhost:4040/)

* Note: The spark ui will contain a new tab called "Connect"


## Conclusion

Due to the lack of documentation and examples I had to do a lot of trial and error to get everything working.  I am also not sure if this is the best way to setup a spark cluster with spark connect.  There were even a few things I couldn't get working suck as the delta-spark python library which currently has an open issue on the [GitHub Repo](https://github.com/delta-io/delta/issues/1967)

Hopefully this post will provide more discussion around the topic and help others.  If you have any questions or comments please leave them below or contribute to the [GitHub Repo](https://github.com/gardnmi/spark-connect-local-env)