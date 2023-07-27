In [Quick Start](../../README.md#quick-start), we start a `local[*]` PySpark Session for data explorations in JupyterLab, and a `local[*]` Spark Connect Server. This example will combine both of the above on k8s: A PySpark Connect client from JupyterLab on k8s, connect to a Spark Connect Server on k8s.

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
kubectl apply -f example/jupyter-sparglim-sc/k8s/jupyter-sparglim/
kubectl apply -f example/jupyter-sparglim-sc/k8s/sparglim-server/
```

Check pod is running:

```
$: kubectl get pod -n sparglim

NAME                                           READY   STATUS    RESTARTS   AGE
sparglim-app-5499f54f6b-gk4xv                  1/1     Running   0          33m
```

Access JupyterLab and try it out:

`http://<master-ip>:30888`

Access SparkUI:
`http://<master-ip>:30040`

# Usage

## Code

Using code for `spark on k8s` initialization

```python
from sparglim.config.builder import ConfigBuilder
spark = ConfigBuilder().config_connect_client().get_or_create()
```

## SQL

This will auto config SparkSession to `connect_client` mode, via env `SPARGLIM_SQL_MODE`

```python
%load_ext sparglim.sql
from sparglim.config.builder import ConfigBuilder
spark = ConfigBuilder().get_or_create() # No need to config_connect_client(), ConfigBuilder is a Singleton
```

Test it:

```python
%sql SHOW TABLES
```

# TIPS

Any configuration on the client side, such as `spark.sql.repl.eagerEval.enabled=true`, is not effective. So `%sql`(`%%sql`) can't display the dataframe. You can use `df.show()` instead.
