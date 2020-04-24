from raydp.services import MasterService
from raydp.spark.utils import register_exit_handler
from typing import Any, Dict

import logging
import os
import ray
import subprocess
import tempfile


class SparkMasterService(MasterService):
    def __init__(self,
                 spark_home: str,
                 port: Any = None,
                 webui_port: Any = None,
                 properties: Dict[str, str] = None):
        self._spark_home = spark_home
        self._host = self.get_host()
        self._port = port
        self._webui_port = webui_port
        self._properties = properties
        self._properties_file = None
        self._start_up = False

        os.environ["SPARK_HOME"] = self._spark_home
        os.environ["SPARK_MASTER_IP"] = self._host

    def start_up(self) -> str:
        if self._start_up:
            logging.warning("The master has started up already.")
            return None

        args = [f"{self._spark_home}/sbin/start-master.sh",
                "--host", self._host]

        if self._port:
            args.append("--port")
            args.append(str(self._port))

        if self._webui_port:
            args.append("--webui-port")
            args.append(str(self._webui_port))

        if self._properties:
            self._properties_file = tempfile.NamedTemporaryFile(delete=False)
            try:
                for k, v in self._properties.items():
                    self._properties_file.write(f"{k}\t{v}".encode("utf-8"))
            except Exception as exp:
                self._properties_file.close()
                self._properties_file = None
                os.remove(self._properties_file)
                return f"Write properties to temp file failed. {exp}"
            finally:
                if self._properties_file:
                    self._properties_file.close()

            if self._properties_file:
                args.append("--properties-file")
                args.append(self._properties_file)

        try:
            args = " ".join(args)
            logging.info(f"Start up master: {args}")
            subprocess.check_call(args=args, shell=True)
        except Exception as exp:
            if self._properties_file:
                os.remove(self._properties_file)
            return f"Start up master failed: {exp}"
        else:
            self._start_up = True
            # register stop when the worker exist
            register_exit_handler(self.stop)
            return None

    def get_master_url(self) -> str:
        if self._start_up:
            return f"spark://{self._host}:{self._port}"
        else:
            return ""

    def get_host(self) -> str:
        # use the same ip as the ray worker
        return ray.worker.global_worker.node_ip_address

    def stop(self):
        if self._start_up:
            args = f"{self._spark_home}/sbin/stop-master.sh"
            try:
                logging.info(f"Stop master: {args}")
                subprocess.check_call(args=args, shell=True)
            except Exception as exp:
                # TODO: force kill?
                logging.error(f"Stop master failed: {exp}")
            finally:
                if self._properties_file:
                    os.remove(self._properties_file)
            self._start_up = False
