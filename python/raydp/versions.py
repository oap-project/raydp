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

import re
import pyspark


# log4j1 if spark version <= 3.2, otherwise, log4j2
SPARK_LOG4J_VERSION = "log4j"
SPARK_LOG4J_CONFIG_FILE_NAME_KEY = "log4j.configurationFile"
SPARK_LOG4J_CONFIG_FILE_NAME_DEFAULT = "log4j-default.properties"
_spark_ver = re.search("\\d+\\.\\d+", pyspark.version.__version__)
if _spark_ver.group(0) > "3.2":
    SPARK_LOG4J_VERSION = "log4j2"
    SPARK_LOG4J_CONFIG_FILE_NAME_KEY = "log4j2.configurationFile"
    SPARK_LOG4J_CONFIG_FILE_NAME_DEFAULT = "log4j2-default.properties"

# support ray >= 2.1, they all use log4j2
RAY_LOG4J_VERSION = "log4j2"
RAY_LOG4J_CONFIG_FILE_NAME_KEY = "log4j2.configurationFile"
RAY_LOG4J_CONFIG_FILE_NAME_DEFAULT = "log4j2.xml"
