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

from typing import Any, List, Optional, TypeVar

import torch
from torch.utils.data import IterableDataset

from raydp.parallel.dataset import Shard

T = TypeVar("T")


class IteratorDataset(IterableDataset):
    def __init__(self,
                 shard: Shard[T],
                 types: Optional[List["torch.dtype"]] = None,
                 shapes: Optional[List[Any]] = None):
        self._shard = shard
        self._types = types
        self._shapes = shapes

    def __iter__(self):
        base_iter = self._shard.base_iter()
        types = self._types
        shapes = self._shapes
        if types is not None:
            record_len = len(types)
        elif shapes is not None:
            record_len = len(shapes)
        else:
            base_iter = self._shard.buffered().base_iter()
            record_len = len(base_iter.head())

        if types is None:
            types = [torch.float32] * record_len
        if shapes is None:
            shapes = [0] * record_len

        def to_tensors(it):
            for items in it:
                tensors = []
                for item, typ, shape in zip(items, types, shapes):
                    t = torch.as_tensor(item, dtype=typ)
                    if shape != [0]:
                        t = t.view(*(-1, *shape))
                    tensors.append(t)
                yield (*tensors[0: -1], tensors[-1])

        tensor_iter = to_tensors(base_iter)
        return tensor_iter
