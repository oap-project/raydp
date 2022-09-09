from os.path import dirname
import sys
import json
import subprocess
import ray
import pyspark

ray.init(address="auto")
node = ray.worker.global_worker.node
options = {}
options["ray"] = {}
options["ray"]["run-mode"] = "CLUSTER"
options["ray"]["node-ip"] = node.node_ip_address
options["ray"]["address"] = node.address
options["ray"]["session-dir"] = node.get_session_dir_path()

ray.shutdown()
conf_path = dirname(__file__) + "/ray.conf"
with open(conf_path, "w") as f:
    json.dump(options, f)
command = ["bin/raydp-submit", "--ray-conf", conf_path]
command += ["--conf", "spark.executor.cores=1"]
command += ["--conf", "spark.executor.instances=1"]
command += ["--conf", "spark.executor.memory=500m"]
example_path = dirname(pyspark.__file__)
# run SparkPi as example
command.append(example_path + "/examples/src/main/python/pi.py")
sys.exit(subprocess.run(command, check=True).returncode)
