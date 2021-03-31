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

import os
import sys

from raydp.mpi import constants
from raydp.mpi import network
from raydp.mpi import protocol
from raydp.mpi.network import get_environ_value

if __name__ == "__main__":
    # pop the file name
    argv = sys.argv[1:]
    # get the host name, the tail is the command
    host_name = argv.pop(0)

    # get the driver information to connect
    job_id = get_environ_value(constants.MPI_JOB_ID)
    driver_host = get_environ_value(constants.MPI_DRIVER_HOST)
    driver_port = int(get_environ_value(constants.MPI_DRIVER_PORT))

    network_timeout = int(os.environ.get(constants.NETWORK_TIME_OUT, "1"))
    op_timeout = int(os.environ.get(constants.MAXIMUM_WAIT_TIME_OUT, "1"))

    client = network.BlockedWorker(job_id=job_id,
                                   name="rsh_agent_" + host_name,
                                   host=driver_host,
                                   port=driver_port,
                                   timeout=network_timeout,
                                   max_wait_timeout=op_timeout)

    client.send(protocol.AgentRegister(job_id, host_name, " ".join(argv)))
    client.close()
