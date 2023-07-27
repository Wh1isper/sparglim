This is development verification for [examples/jupyter-sparglim-on-k8s](../../examples/jupyter-sparglim-on-k8s)

Use  [docker/Dockerfile.jupyterlab-sparglim](../docker/Dockerfile.jupyterlab-sparglim) to build a dev version `jupyterlab-sparglim`.

```
# In project root dir
docker build -t wh1isper/jupyterlab-sparglim:dev -f dev/docker/Dockerfile.jupyterlab-sparglim .

# reload by deleting deployment pod
./dev/scripts/reload.sh
```
