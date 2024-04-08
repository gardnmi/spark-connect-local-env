# spark-connect-local-env

[![PyPI - Version](https://img.shields.io/pypi/v/pyspark-delta-no-databricks.svg)](https://pypi.org/project/pyspark-delta-no-databricks)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pyspark-delta-no-databricks.svg)](https://pypi.org/project/pyspark-delta-no-databricks)

-----

**Table of Contents**

- [Installation](#installation)
- [License](#license)

## Requirements
* Docker
* Visual Studio Code
* Visual Studio Code Extensions:
    * Dev Conainers


## Installation

1. Clone this Repo:

```console
git clone https://github.com/gardnmi/spark-connect-local-env.git
```
2. Open the project in Visual Studio Code
3. Open the command palette (Ctrl+Shift+P) and run the command `Remote-Containers: Reopen in Container`


## How to use

After Docker container is running, open you broswer and got to:

1. [localhost:8080](http://localhost:8080/) - Spark Master
2. [localhost:4040](http://localhost:4040/) - Spark UI
3. [localhost:9000](http://localhost:9000/) - Minio Web Console

To log into the Minio web console use the default credentials:
* User: `minioadmin`
* Password: `minioadmin`

Run the python main.py file to create a dataframe and write a delta table to Minio.


## License

`spark-connect-local-env` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.


## Resources:
* [Compatablity Matrix for Delta Lake](https://docs.delta.io/latest/releases.html)
* [Fix Windows Issue with bitnami dockerfile](https://github.com/bitnami/containers/issues/63510)
* [Another Spark Env for Reference](https://github.com/emmc15/pyspark-testing-env/blob/main/docker-compose.yml)
* [Bitnami Spark Image](https://github.com/bitnami/containers/blob/main/bitnami/spark/README.md)
* [Delta Lake and Minio Configs](https://stackoverflow.com/questions/75472225/java-lang-classnotfoundexception-class-org-apache-hadoop-fs-s3a-s3afilesystem-n)