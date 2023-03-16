#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import json
import logging
import os
import shutil
import signal
import struct
import tempfile
import time
from subprocess import Popen, PIPE
import glob
import ray
from py4j.java_gateway import JavaGateway, GatewayParameters
from raydp import versions

logger = logging.getLogger(__name__)

RAYDP_SPARK_MASTER_SUFFIX = "_SPARK_MASTER"
RAYDP_EXECUTOR_EXTRA_CLASSPATH = "raydp.executor.extraClassPath"
SPARK_JAVAAGENT = "spark.javaagent"
SPARK_RAY_LOG_PREFER_CLASSPATH = "spark.ray.logPreferClassPath"
SPARK_RAY_LOG4J_FACTORY_CLASS_KEY = "spark.ray.log4j.factory.class"
SPARK_LOG4J_CONFIG_FILE_NAME = "spark.log4j.config.file.name"
RAY_LOG4J_CONFIG_FILE_NAME = "spark.ray.log4j.config.file.name"



@ray.remote
class RayDPSparkMaster():
    def __init__(self, configs):
        self._gateway = None
        self._app_master_java_bridge = None
        self._host = None
        self._started_up = False
        self._configs = configs
        self._spark_home = None
        self._objects = {}
        self._actor_id = None

    def start_up(self, popen_kwargs=None):
        if self._started_up:
            logger.warning("The RayClusterMaster has started already. Do not call it twice")
            return
        extra_classpath = os.pathsep.join(self._prepare_jvm_classpath())
        self._gateway = self._launch_gateway(extra_classpath, popen_kwargs)
        self._app_master_java_bridge = self._gateway.entry_point.getAppMasterBridge()
        ray_properties = self._generate_ray_configs()
        self._gateway.jvm.org.apache.spark.deploy.raydp.RayAppMaster.setProperties(ray_properties)
        self._host = ray.util.get_node_ip_address()
        self._create_app_master(extra_classpath)
        self._started_up = True

    def _prepare_jvm_classpath(self):
        # pylint: disable=import-outside-toplevel,multiple-imports,cyclic-import
        import raydp, pyspark
        raydp_cp = os.path.abspath(os.path.join(os.path.dirname(raydp.__file__), "jars/*"))
        self._spark_home = os.environ.get("SPARK_HOME", os.path.dirname(pyspark.__file__))
        spark_jars_dir = os.path.abspath(os.path.join(self._spark_home, "jars/*"))
        ray_cp = os.path.abspath(os.path.join(os.path.dirname(ray.__file__), "jars/*"))
        raydp_jars = glob.glob(raydp_cp)
        spark_jars = glob.glob(spark_jars_dir)
        ray_jars = glob.glob(ray_cp)

        cp_list = []

        if RAYDP_EXECUTOR_EXTRA_CLASSPATH in self._configs:
            user_cp = self._configs[RAYDP_EXECUTOR_EXTRA_CLASSPATH].rstrip(os.pathsep)
            cp_list.extend(user_cp.split(os.pathsep))

        cp_list.extend(raydp_jars)

        # set cp order based on log prefer path
        if "spark" == self._configs[SPARK_RAY_LOG_PREFER_CLASSPATH]:
            cp_list.extend(spark_jars)
            cp_list.extend(ray_jars)
        else:
            log_cp = self._configs[SPARK_RAY_LOG_PREFER_CLASSPATH].rstrip(os.pathsep)
            cp_list.extend(log_cp.split(os.pathsep))
            cp_list.extend(spark_jars)
            cp_list.extend(ray_jars)

        return cp_list

    def _launch_gateway(self, class_path, popen_kwargs=None):
        """
        launch jvm gateway
        :param popen_kwargs: Dictionary of kwargs to pass to Popen when spawning
            the py4j JVM. This is a developer feature intended for use in
            customizing how pyspark interacts with the py4j JVM (e.g., capturing
            stdout/stderr).
        """
        env = dict(os.environ)

        command = ["java"]

        # append JAVA_OPTS. This can be used for debugging.
        if "JAVA_OPTS" in env:
            command.append(env["JAVA_OPTS"])
        # set system class loader and log prefer class path
        import ray
        logging_dir = ray._private.worker._global_node.get_logs_dir_path()
        command.append("-javaagent:" + self._configs[SPARK_JAVAAGENT])
        command.append("-Dray.logging.dir" + "=" + logging_dir)
        command.append("-D" + SPARK_RAY_LOG4J_FACTORY_CLASS_KEY + "="
                       + self._configs[SPARK_RAY_LOG4J_FACTORY_CLASS_KEY])
        command.append("-D" + versions.RAY_LOG4J_CONFIG_FILE_NAME_KEY + "="
                       + self._configs[RAY_LOG4J_CONFIG_FILE_NAME])
        command.append("-cp")
        command.append(class_path)
        command.append("org.apache.spark.deploy.raydp.AppMasterEntryPoint")

        # Create a temporary directory where the gateway server should write the connection
        # information.
        conn_info_dir = tempfile.mkdtemp()
        try:
            fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
            os.close(fd)
            os.unlink(conn_info_file)

            env["_RAYDP_APPMASTER_CONN_INFO_PATH"] = conn_info_file

            # Launch the Java gateway.
            popen_kwargs = {} if popen_kwargs is None else popen_kwargs
            # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
            popen_kwargs["stdin"] = PIPE
            # We always set the necessary environment variables.
            popen_kwargs["env"] = env

            # Don't send ctrl-c / SIGINT to the Java gateway:
            def preexec_func():
                signal.signal(signal.SIGINT, signal.SIG_IGN)
            popen_kwargs["preexec_fn"] = preexec_func
            # pylint: disable=R1732
            proc = Popen(command, **popen_kwargs)

            # Wait for the file to appear, or for the process to exit, whichever happens first.
            while not proc.poll() and not os.path.isfile(conn_info_file):
                time.sleep(0.1)

            if not os.path.isfile(conn_info_file):
                raise Exception("Java gateway process exited before sending its port number")

            with open(conn_info_file, "rb") as info:
                length = info.read(4)
                if not length:
                    raise EOFError
                gateway_port = struct.unpack("!i", length)[0]

        finally:
            shutil.rmtree(conn_info_dir)

        gateway = JavaGateway(gateway_parameters=GatewayParameters(
            port=gateway_port, auto_convert=True))

        # Store a reference to the Popen object for use by the caller
        # (e.g., in reading stdout/stderr)
        gateway.proc = proc

        return gateway

    def _generate_ray_configs(self):
        assert ray.is_initialized()
        options = {}

        node = ray._private.worker._global_node
        options["ray.run-mode"] = "CLUSTER"
        options["ray.node-ip"] = node.node_ip_address
        options["ray.address"] = node.address
        options["ray.logging.dir"] = node.get_logs_dir_path()
        options["ray.session-dir"] = node.get_session_dir_path()
        options["ray.raylet.node-manager-port"] = node.node_manager_port
        options["ray.raylet.socket-name"] = node.raylet_socket_name
        options["ray.raylet.config.num_workers_per_process_java"] = "1"
        options["ray.object-store.socket-name"] = node.plasma_store_socket_name
        options["ray.logging.level"] = "INFO"
        options["ray.job.namespace"] = ray.get_runtime_context().namespace

        # jnius_config.set_option has some bug, we set this options in java side
        return json.dumps(options)

    def get_host(self) -> str:
        assert self._started_up
        return self._host

    def _create_app_master(self, extra_classpath: str):
        if self._started_up:
            return
        self._app_master_java_bridge.startUpAppMaster(extra_classpath, self._configs)

    def get_master_url(self):
        assert self._started_up
        return self._app_master_java_bridge.getMasterUrl()

    def get_spark_home(self) -> str:
        assert self._started_up
        return self._spark_home

    def add_objects(self, timestamp, objects):
        self._objects[timestamp] = objects

    def get_object(self, timestamp, idx):
        return self._objects[timestamp][idx]

    def get_ray_address(self):
        return ray.worker.global_worker.node.address

    def get_actor_id(self):
        if self._actor_id is None:
            self._actor_id = ray.get_runtime_context().actor_id
        return self._actor_id

    def stop(self, cleanup_data):
        self._started_up = False
        if self._app_master_java_bridge is not None:
            self._app_master_java_bridge.stop()
            self._app_master_java_bridge = None

        if self._gateway is not None:
            self._gateway.shutdown()
            self._gateway.proc.terminate()
            self._gateway = None
        if cleanup_data:
            ray.actor.exit_actor()
