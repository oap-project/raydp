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

import logging
import os
import subprocess
import tempfile
from typing import Any, Dict

import ray

from raydp.services import ClusterWorker
from raydp.spark.utils import register_exit_handler


class StandaloneWorkerService(ClusterWorker):
    def __init__(self,
                 master_url: str,
                 cores: int,
                 memory: int,
                 spark_home: str,
                 port: Any = None,
                 webui_port: Any = None,
                 work_dir: str = None,
                 properties: Dict[str, str] = None):
        self._master_url = master_url
        self._cores = cores
        self._memory = memory
        self._spark_home = spark_home
        self._host = self.get_host()
        self._port = port
        self._webui_port = webui_port
        self._work_dir = work_dir
        self._properties = properties
        self._properties_file = None
        self._start_up = False

        os.environ["SPARK_HOME"] = self._spark_home

    def start_up(self) -> str:
        if self._start_up:
            logging.warning("The worker has started up already.")
            return None

        args = [f"{self._spark_home}/sbin/start-slave.sh", self._master_url,
                "--cores", str(self._cores),
                "--memory", str(self._memory),
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
                return f"Write properties to temp file failed: {exp}"
            finally:
                if self._properties_file:
                    self._properties_file.close()

            if self._properties_file:
                args.append("--properties-file")
                args.append(self._properties_file)

        if self._work_dir:
            args.append("--work-dir")
            args.append(self._work_dir)

        try:
            args = " ".join(args)
            logging.info(f"Start up worker: {args}")
            subprocess.check_call(args=args, shell=True)
        except Exception as exp:
            if self._properties_file:
                os.remove(self._properties_file)
            return f"Start up worker failed: {exp}"
        else:
            self._start_up = True
            # register stop when the worker exist
            register_exit_handler(self.stop)
            return None

    def get_host(self) -> str:
        # use the same ip as ray worker
        return ray.worker.global_worker.node_ip_address

    def stop(self):
        if self._start_up:
            args = f"{self._spark_home}/sbin/stop-slave.sh"
            try:
                logging.info(f"Stop worker: {args}")
                subprocess.check_call(args=args, shell=True)
            except Exception as exp:
                # TODO: force kill?
                logging.error(f"Stop worker failed: {exp}")
            finally:
                if self._properties_file:
                    os.remove(self._properties_file)
            self._start_up = False
