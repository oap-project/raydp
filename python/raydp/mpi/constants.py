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

from os import path

MPI_TYPE = "raydp_mpi_type"
MPI_JOB_ID = "raydp_mpi_job_id"
MPI_DRIVER_HOST = "raydp_mpi_driver_host"
MPI_DRIVER_PORT = "raydp_mpi_driver_port"

MAXIMUM_WAIT_TIME_OUT = "raydp_maximum_wait_time_out"

_current_dir = path.dirname(path.realpath(__file__))
MPI_MAIN_CLASS_PATH = path.join(_current_dir, "mpi_worker.py")
