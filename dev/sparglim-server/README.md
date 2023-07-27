This is development verification for [examples/sparglim-server](../../examples/sparglim-server)

Use [docker/Dockerfile.sparglim-server](../docker/Dockerfile.sparglim-server) to build a dev version `sparglim-server`

```
# In project root dir
docker build -t wh1isper/sparglim-server:dev -f dev/docker/Dockerfile.sparglim-server .
# reload by deleting deployment pod
./dev/scripts/reload.sh

```
