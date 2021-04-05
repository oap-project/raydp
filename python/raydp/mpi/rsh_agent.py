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

import sys

from raydp.mpi import constants
from raydp.mpi.network import network_pb2, network_pb2_grpc
from raydp.mpi.utils import create_insecure_channel, get_environ_value

if __name__ == "__main__":
    # pop the file name
    argv = sys.argv[1:]
    # get the host name, the tail is the command
    host_name = argv.pop(0)

    # get the driver information to connect
    job_id = get_environ_value(constants.MPI_JOB_ID)
    driver_host = get_environ_value(constants.MPI_DRIVER_HOST)
    driver_port = int(get_environ_value(constants.MPI_DRIVER_PORT))

    with create_insecure_channel(f"{driver_host}:{driver_port}") as channel:
        stub = network_pb2_grpc.DriverServiceStub(channel)
        register_msg = network_pb2.AgentRegisterRequest(job_id=job_id,
                                                        name=host_name,
                                                        command=" ".join(argv))
        reply = stub.RegisterAgent(register_msg,
                                    wait_for_ready=True)
        # we can do nothing if register failed
        assert reply.succeed
        print("Agent register succeed")
