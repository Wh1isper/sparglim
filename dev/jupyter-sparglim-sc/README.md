This is development verification for [examples/jupyter-sparglim-sc](../../examples/jupyter-sparglim-sc)

Use  [docker/Dockerfile.jupyterlab-sparglim](../docker/Dockerfile.jupyterlab-sparglim) and [docker/Dockerfile.sparglim-server](../docker/Dockerfile.sparglim-server)  and to build a dev version `jupyterlab-sparglim` and  `sparglim-server`

```
# In project root dir
docker build -t wh1isper/jupyterlab-sparglim:dev -f dev/docker/Dockerfile.jupyterlab-sparglim .
docker build -t wh1isper/sparglim-server:dev -f dev/docker/Dockerfile.sparglim-server .


# reload by deleting deployment pod
./dev/scripts/reload.sh
```
