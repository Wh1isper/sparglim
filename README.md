![](https://img.shields.io/github/license/wh1isper/sparglim)
![](https://img.shields.io/github/v/release/wh1isper/sparglim?logo=github)
![](https://img.shields.io/github/v/release/wh1isper/sparglim?include_prereleases&label=pre-release&logo=github)
![](https://img.shields.io/pypi/dm/sparglim)
![](https://img.shields.io/github/last-commit/wh1isper/sparglim)
![](https://img.shields.io/pypi/pyversions/sparglim)

# Sparglim ✨

Sparglim is aimed at providing a clean solution for PySpark applications in cloud-native scenarios (On K8S、Connect Server etc.).

**This is a fledgling project, looking forward to any PRs, Feature Requests and Discussions!**

🌟✨⭐ Start to support!

## Quick Start

Run Jupyterlab with `sparglim` docker image:

```bash
docker run \
-it \
-p 8888:8888 \
wh1isper/jupyterlab-sparglim
```

Access `http://localhost:8888` in browser to use jupyterlab with `sparglim`. Then you can try [SQL Magic](#sql-magic).

Run and Daemon a Spark Connect Server:

```bash
docker run \
-it \
-p 15002:15002 \
-p 4040:4040 \
wh1isper/sparglim-server
```

Access `http://localhost:4040` for Spark-UI and `sc://localhost:15002` for Spark Connect Server. [Use sparglim to setup SparkSession to connect to Spark Connect Server](#connect-to-spark-connect-server).

## Install: `pip install sparglim[all]`

- Install only for config and daemon spark connect server `pip install sparglim`
- Install for pyspark app `pip install sparglim[pyspark]`
- Install for using magic within ipython/jupyter (will also install pyspark) `pip install sparglim[magic]`
- **Install for all above** (such as using magic in jupyterlab on k8s) `pip install sparglim[all]`

## Feature

- Config Spark via environment variables, see [config spark](./config.md)
- `%SQL` and `%%SQL` magic for executing Spark SQL in IPython/Jupyter
  - SQL statement can be written in multiple lines, support using `;` to separate statements
  - Support config `connect client`, see [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html#spark-connect-overview)
  - *TODO: Visualize the result of SQL statement(Spark Dataframe)*
- `sparglim-server` for daemon Spark Connect Server

## User cases

### PySpark App

To config Spark on k8s for Data explorations, see [examples/jupyter-sparglim-on-k8s](./examples/jupyter-sparglim-on-k8s)

*TODO: To config Spark for ELT Application/Service, see [pyspark-sampling](https://github.com/Wh1isper/pyspark-sampling/)*

### Spark Connect Server on K8S

To daemon Spark Connect Server on K8S, see [examples/sparglim-server](./examples/sparglim-server)

To daemon Spark Connect Server on K8S and Connect it in JupyterLab , see [examples/jupyter-sparglim-sc](./examples/jupyter-sparglim-sc)

### Connect to Spark Connect Server

Only thing need to do is to set `SPARGLIM_REMOTE` env, format is `sc://host:port`

Example Code:

```python
import os
os.environ["SPARGLIM_REMOTE"] = "sc://localhost:15002" # or export SPARGLIM_REMOTE=sc://localhost:15002 before run python

from sparglim.config.builder import ConfigBuilder
from datetime import datetime, date
from pyspark.sql import Row


c = ConfigBuilder().config_connect_client()
spark = c.get_or_create()

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])
df.show()

```

### SQL Magic

Install Sparglim with

```bash
pip install sparglim["magic"]
```

Load magic in IPython/Jupyter

```ipython
%load_ext sparglim.sql
```

Create a view:

```python
from sparglim.config.builder import ConfigBuilder


from datetime import datetime, date
from pyspark.sql import Row

c: ConfigBuilder = ConfigBuilder()
spark = c.get_or_create()


df = spark.createDataFrame([
            Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
            Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
            Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
        ])
df.createOrReplaceTempView("tb")

```

Query the view by `%SQL`:

```ipython
%sql SELECT * FROM tb
```

`%SQL` result dataframe can be assigned to a variable:

```ipython
df = %sql SELECT * FROM tb
df
```

or `%%SQL` can be used to execute multiple statements:

```ipython
%%sql SELECT
        *
        FROM
        tb;
```

You can also using Spark SQL to load data from external data source, such as:

```ipython
%%sql CREATE TABLE tb_people
USING json
OPTIONS (path "/path/to/file.json");
Show tables;
```

## Develop

Install pre-commit before commit

```
pip install pre-commit
pre-commit install
```

Install package locally

```
pip install -e .[test]
```

Run unit-test before PR, **ensure that new features are covered by unit tests**

```
pytest -v
```

(Optional, python<=3.10) Use [pytype](https://github.com/google/pytype) to check typed

```
pytype ./sparglim
```
