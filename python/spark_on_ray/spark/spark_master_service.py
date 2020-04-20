from spark_on_ray.services import MasterService
from typing import Any, Dict

import atexit
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

        atexit.register(self.stop)

    def start_up(self) -> bool:
        if self._start_up:
            # TODO: add warning?
            return True

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
            except:
                self._properties_file.close()
                self._properties_file = None
                os.remove(self._properties_file)
                raise Exception("Write properties to temp file failed.")
            finally:
                if self._properties_file:
                    self._properties_file.close()

            if self._properties_file:
                args.append("--properties-file")
                args.append(self._properties_file)

        try:
            args = " ".join(args)
            subprocess.check_call(args=args, shell=True)
        except:
            if self._properties_file:
                os.remove(self._properties_file)
            raise
        else:
            self._start_up = True
            return True

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
                subprocess.check_call(args=args, shell=True)
            except:
                # TODO: force kill?
                pass
            finally:
                if self._properties_file:
                    os.remove(self._properties_file)
            self._start_up = False
