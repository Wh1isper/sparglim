In [Quick Start](../../README.md#quick-start), we start a `local[*]` Spark Connect Server. This example will show how to start a Spark Connect Server on k8s.

# Prepare

## Namespace: `sparglim`

```
kubectl create ns sparglim
```

## Grant authorization

You need to authorize the pod so that it can create pods(executor)

For a simple test, you can grant administrator privileges to all pods using the following command (**DO NOT this in a production environment**)

```
kubectl create clusterrolebinding serviceaccounts-cluster-admin
  --clusterrole=cluster-admin
  --group=system:serviceaccounts
```

# Apply and access

```
# In project root
kubectl apply -f example/sparglim-server/k8s
```

Check pod is running:

```
$: kubectl get pod -n sparglim

NAME                                           READY   STATUS    RESTARTS   AGE
sparglim-server-5696c9466d-s75bh               1/1     Running   0          86s
spark-connect-server-6c5a798995af404f-exec-1   1/1     Running   0          52s
spark-connect-server-6c5a798995af404f-exec-2   1/1     Running   0          52s
spark-connect-server-6c5a798995af404f-exec-3   1/1     Running   0          52s
```

Access SparkUI:
`http://<master-ip>:30040`


# Connect it with `sparglim`

```python
import os
os.environ["SPARGLIM_REMOTE"] = "sc://<master-ip>:30052" # Also avaliable `export SPARGLIM_REMOTE=sc://<master-ip>:30052` before start python

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
