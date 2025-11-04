from os.path import dirname
import sys
import json
import subprocess
import shlex
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

# JDK 17+ requires --add-opens for reflective access and --add-exports for direct access
# to internal JDK modules. These are needed for Spark, Ray serialization, and RayDP.
java_opts = " ".join([
    "-XX:+IgnoreUnrecognizedVMOptions",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.math=ALL-UNNAMED",
    "--add-opens=java.base/java.text=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
])

command = ["bin/raydp-submit", "--ray-conf", conf_path]
command += ["--conf", "spark.executor.cores=1"]
command += ["--conf", "spark.executor.instances=1"]
command += ["--conf", "spark.executor.memory=500m"]
command += ["--conf", f"spark.executor.extraJavaOptions={java_opts}"]
command += ["--conf", f"spark.driver.extraJavaOptions={java_opts}"]
command += ["--conf", f"spark.ray.raydp_app_master.extraJavaOptions={java_opts}"]
example_path = dirname(pyspark.__file__)
# run SparkPi as example
command.append(example_path + "/examples/src/main/python/pi.py")
cmd_str = " ".join(shlex.quote(arg) for arg in command)
sys.exit(subprocess.run(cmd_str, check=True, shell=True).returncode)
