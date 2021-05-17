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

from typing import Any

import ray
from ray.util.iter import ParallelIteratorWorker


class MLDatasetWorker(ParallelIteratorWorker):
    def __init__(self):
        super().__init__(None, False)

    def get_node_ip(self):
        return ray.services.get_node_ip_address()

    def init(self, item_generator: Any, repeat: bool):
        def make_iterator():
            if callable(item_generator):
                return item_generator()
            else:
                return item_generator

        if repeat:

            def cycle():
                while True:
                    it = iter(make_iterator())
                    if it is item_generator:
                        raise ValueError(
                            ("Cannot iterate over {} multiple times." +
                             "Please pass in the base iterable or" +
                             "lambda: {} instead.").format(
                                item_generator, item_generator))
                    for item in it:
                        yield item

            self.item_generator = cycle()
        else:
            self.item_generator = make_iterator()
