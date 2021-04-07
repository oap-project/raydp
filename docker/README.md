# Running RayDP on k8s cluster

## Build docker image
Building docker image with the following command, and this will create a image tag with `oap-project/raydp:latest`
```shell
# under ${RAYDP_HOME}/docker
./build-docker.sh
```

Then you can push the built image to repository or spread to the k8s worker nodes.

## Start up Ray cluster
```shell
ray up ${RAYDP_HOME}/docker/example.yaml
```
The above command will print the steps that you can attach or submit job to the cluster.
## Connect to the Ray cluster
```shell
ray attach ${RAYDP_HOME}/docker/example.yaml
```
After you attached to the head node, you can program as normally. The following is an example.
```python
import ray
import raydp

# connect to the cluster
ray.init(address='auto', _redis_password='the password printed when you start up the cluster')

# init spark
spark = raydp.init_spark("K8S test", num_executors=2, executor_cores=1, executor_memory="512M")
spark.range(0, 100).count()
```

## Destroy the Ray cluster
```
ray down ${RAYDP_HOME}/docker/example.yaml
```
