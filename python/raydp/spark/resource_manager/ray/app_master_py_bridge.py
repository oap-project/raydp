import json
import os
import shutil
import signal
import struct
import tempfile
from subprocess import Popen, PIPE

import ray
import time
from py4j.java_gateway import JavaGateway, GatewayParameters
from pyspark import find_spark_home


class AppMasterPyBridge:
    def __init__(self, popen_kwargs=None):
        self._extra_classpath = os.pathsep.join(self._prepare_jvm_classpath())
        self._gateway = self._launch_gateway(self._extra_classpath, popen_kwargs)
        self._app_master_java_bridge = self._gateway.entry_point.getAppMasterBridge()
        self._set_properties()

    def _prepare_jvm_classpath(self):
        cp_list = []
        # find ray jar path
        cp_list.extend(ray.services.DEFAULT_JAVA_WORKER_CLASSPATH)
        # find RayDP core path
        current_path = os.path.abspath(__file__)
        raydp_cp = os.path.abspath(os.path.join(current_path, "../../../../jars/*"))
        cp_list.append(raydp_cp)
        # find pyspark jars path
        spark_home = find_spark_home._find_spark_home()
        spark_jars = os.path.join(spark_home, "jars/*")
        cp_list.append(spark_jars)
        return cp_list

    def _launch_gateway(self, class_path, popen_kwargs=None):
        """
        launch jvm gateway
        :param popen_kwargs: Dictionary of kwargs to pass to Popen when spawning
            the py4j JVM. This is a developer feature intended for use in
            customizing how pyspark interacts with the py4j JVM (e.g., capturing
            stdout/stderr).
        """

        command = ["java"]
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

            env = dict(os.environ)
            env["_RAYDP_APPMASTER_CONN_INFO_PATH"] = conn_info_file

            # Launch the Java gateway.
            popen_kwargs = {} if popen_kwargs is None else popen_kwargs
            # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
            popen_kwargs['stdin'] = PIPE
            # We always set the necessary environment variables.
            popen_kwargs['env'] = env

            # Don't send ctrl-c / SIGINT to the Java gateway:
            def preexec_func():
                signal.signal(signal.SIGINT, signal.SIG_IGN)
            popen_kwargs['preexec_fn'] = preexec_func
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
                gateway_port =  struct.unpack("!i", length)[0]

        finally:
            shutil.rmtree(conn_info_dir)

        gateway = JavaGateway(gateway_parameters=GatewayParameters(
            port=gateway_port, auto_convert=True))

        # Store a reference to the Popen object for use by the caller (e.g., in reading stdout/stderr)
        gateway.proc = proc

        return gateway

    def _set_properties(self):
        options = {}

        node = ray.worker.global_worker.node

        options["ray.node-ip"] = node.node_ip_address
        options["ray.redis.address"] = node.redis_address
        options["ray.redis.password"] = node.redis_password
        # options["ray.redis.head-password"] = node.redis_password
        options["ray.logging.dir"] = node.get_session_dir_path()
        options["ray.raylet.node-manager-port"] = node.node_manager_port
        options["ray.raylet.socket-name"] = node.raylet_socket_name
        options["ray.raylet.config.num_workers_per_process_java"] = "1"
        options["ray.object-store.socket-name"] = node.plasma_store_socket_name
        options["ray.logging.level"] = "DEBUG"

        # jnius_config.set_option has some bug, we set this options in java side
        jvm_properties = json.dumps(options)
        self._app_master_java_bridge.setProperties(jvm_properties)

    def create_app_master(self):
        self._app_master_java_bridge.startUpAppMaster(self._extra_classpath)

    def get_master_url(self):
        return self._app_master_java_bridge.getMasterUrl()

    def stop(self):
        if self._app_master_java_bridge is not None:
            self._app_master_java_bridge.stop()
            self._app_master_java_bridge = None

        if self._gateway is not None:
            self._gateway.shutdown()
            self._gateway = None
