In [Quick Start](../../README.md#quick-start), we start a `local[*]` PySpark Session for data explorations in JupyterLab. This example is using  `spark on k8s for same purpose`.

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
kubectl apply -f example/jupyter-sparglim-on-k8s/k8s
```

Check pod is running:

```
$: kubectl get pod -n sparglim

NAME                                           READY   STATUS    RESTARTS   AGE
sparglim-app-5499f54f6b-gk4xv                  1/1     Running   0          33m
```

Access JupyterLab and try it out:

`http://<master-ip>:30888`

# Usage

## Code

Using code for `spark on k8s` initialization

```
from sparglim.config.builder import ConfigBuilder
spark = ConfigBuilder().config_k8s().get_or_create()
```

When SparkSession created, check executor is up:` kubectl get pod -n sparglim`

```
NAME                               READY   STATUS    RESTARTS   AGE
sparglim-825bf989955f3593-exec-1   1/1     Running   0          53m
sparglim-825bf989955f3593-exec-2   1/1     Running   0          53m
sparglim-825bf989955f3593-exec-3   1/1     Running   0          53m
sparglim-app-8495f7b796-2h7sc      1/1     Running   0          53m
```

## SQL

This will auto config SparkSession to `k8s` mode, via env `SPARGLIM_SQL_MODE`

```python
%load_ext sparglim.sql
from sparglim.config.builder import ConfigBuilder
spark = ConfigBuilder().get_or_create() # No need to config_k8s(), ConfigBuilder is a Singleton
```

Test it:

```python
%sql SHOW TABLES;
```


When SparkSession created, check executor is up: `kubectl get pod -n sparglim`

```
NAME                               READY   STATUS    RESTARTS   AGE
sparglim-825bf989955f3593-exec-1   1/1     Running   0          53m
sparglim-825bf989955f3593-exec-2   1/1     Running   0          53m
sparglim-825bf989955f3593-exec-3   1/1     Running   0          53m
sparglim-app-8495f7b796-2h7sc      1/1     Running   0          53m
```
