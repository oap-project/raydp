"""
Test script for raydp-submit --py-files functionality.
This script mimics raydp-submit.py but tests that --py-files works correctly.
"""
from os.path import dirname, abspath, join
import sys
import json
import subprocess
import shlex
import ray

def main():
    print("Starting raydp-submit --py-files test...")

    # Initialize Ray and get cluster info
    ray.init(address="auto")
    node = ray.worker.global_worker.node
    options = {}
    options["ray"] = {}
    options["ray"]["run-mode"] = "CLUSTER"
    options["ray"]["node-ip"] = node.node_ip_address
    options["ray"]["address"] = node.address
    options["ray"]["session-dir"] = node.get_session_dir_path()

    ray.shutdown()

    # Write Ray configuration
    examples_dir = dirname(abspath(__file__))
    conf_path = join(examples_dir, "ray.conf")
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

    # Build raydp-submit command
    command = ["bin/raydp-submit", "--ray-conf", conf_path]
    command += ["--conf", "spark.executor.cores=1"]
    command += ["--conf", "spark.executor.instances=1"]
    command += ["--conf", "spark.executor.memory=500m"]
    command += ["--conf", f"spark.executor.extraJavaOptions={java_opts}"]
    command += ["--conf", f"spark.driver.extraJavaOptions={java_opts}"]
    command += ["--conf", f"spark.ray.raydp_app_master.extraJavaOptions={java_opts}"]

    # Add --py-files with test_pyfile.py
    test_pyfile_path = join(examples_dir, "test_pyfile.py")
    command += ["--py-files", test_pyfile_path]

    # Add the main script
    main_script_path = join(examples_dir, "test_pyfiles_main.py")
    command.append(main_script_path)

    # Execute the command
    print("\nExecuting command:")
    cmd_str = " ".join(shlex.quote(arg) for arg in command)
    print(cmd_str)
    print("\n" + "=" * 60)

    result = subprocess.run(cmd_str, check=True, shell=True)

    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
