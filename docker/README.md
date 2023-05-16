# Running RayDP on k8s cluster

## Build docker image
Build the docker image to use in K8S with the following command, and this will create an image tag with `oap-project/raydp:latest`
```shell
# under ${RAYDP_HOME}/docker
./build-docker.sh
```

You can install our nightly build with `pip install raydp --pre` or `pip install raydp-nightly`.To install raydp-nightly in the image, modify the following code in `Dockerfile`:
```Dockerfile
RUN sudo http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} apt-get update -y \
    && sudo http_proxy=${HTTP_PROXY} https_proxy=${HTTPS_PROXY} apt-get install -y openjdk-8-jdk \
    && sudo mkdir /raydp \
    && sudo chown -R ray /raydp \
    && $HOME/anaconda3/bin/pip --no-cache-dir install raydp-nightly
```

Meanwhile, you should install all dependencies of your application in the `Dockerfile`. If suitable, you can change the base image to `ray-ml`:
```Dockerfile
FROM rayproject/ray-ml:latest
```

Then, you can push the built image to repository or spread to the k8s worker nodes.

## Deploy ray cluster with Helm
You need to create a Helm chart first. To start with, check out this [example ray cluster Helm chart](https://github.com/ray-project/kuberay/tree/master/helm-chart/ray-cluster). You can clone this repo and copy this directory, then modify `values.yaml` to use the previously built image.

```yaml
image:
  repository: oap-project/raydp
  tag: latest
  pullPolicy: IfNotPresent
```

You can also change other fields in this file to specify number of workers, etc.

Then, you need to deploy the KubeRay operator first, plese refer to [here](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started.html#kuberay-quickstart) for instructions. You can now deploy a Ray cluster with RayDP installed via `helm install ray-cluster PATH_to_CHART`.

## Access the cluster
Check here [here](https://docs.ray.io/en/master/cluster/kubernetes/getting-started.html#running-applications-on-a-ray-cluster) to see how to run applications on the cluster you just deployed.

## Legacy
If you are using Ray versions before 2.0, you can try this command.
```shell
ray up ${RAYDP_HOME}/docker/legacy.yaml
```