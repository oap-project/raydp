from services import WorkerService
from typing import Dict

import os
import subprocess
import tempfile


class SparkWorkerService(WorkerService):
    def __init__(self,
                 master_url: str,
                 cores: int,
                 memory: int,
                 spark_home: str,
                 port: int = -1,
                 webui_port: int = 8081,
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
        self._start_up = False

    def start_up(self) -> bool:
        if self._start_up:
            # TODO: add warning?
            return True

        args = [f"{self._spark_home}/sbin/start-slave.sh",
                "--cores", self._cores,
                "--memory", self._memory,
                "--host", self._host,
                "--port", self._port,
                "--webui-port", self._webui_port]

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

        if self._work_dir:
            args.append("--work-dir")
            args.append(self._work_dir)

        try:
            subprocess.call(args=args, shell=True)
        except:
            if self._properties_file:
                os.remove(self._properties_file)
            raise
        else:
            self._start_up = True
            return True

    def kill(self):
        if self._start_up:
            args = [f"{self._spark_home}/sbin/stop-slave.sh"]
            try:
                subprocess.call(args=args, shell=True)
            except:
                # TODO: force kill?
                pass
            finally:
                if self._properties_file:
                    os.remove(self._properties_file)
            self._start_up = False
