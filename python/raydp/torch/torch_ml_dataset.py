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


import logging
import queue
import threading
from typing import Callable

import ray
import torch
from ray.util.data import MLDataset
from torch.utils.data import IterableDataset

logger = logging.getLogger(__name__)


class TorchMLDataset(IterableDataset):
    def __init__(self,
                 ds: MLDataset,
                 collate_fn: Callable,
                 shuffle: bool = False,
                 shuffle_seed: int = None):
        self.ds = ds
        self.collate_fn = collate_fn
        self.shuffle = shuffle
        self.shuffle_seed = shuffle_seed or 1

    def __iter__(self):
        it = self.ds.gather_async(batch_ms=0, num_async=self.ds.num_shards())
        it = iter(it)
        for pdf in it:
            if self.shuffle:
                pdf = pdf.sample(frac=1.0, random_state=self.shuffle_seed)
            yield self.collate_fn(pdf)

    def __len__(self):
        all_actors = []
        for actor_set in self.ds.actor_sets:
            all_actors.extend(actor_set.actors)
        assert len(all_actors) > 0
        if "__len__" in dir(all_actors[0]):
            # This is a very hack method to get the length of the iterator
            num_records = sum([ray.get(actor.__len__.remote()) for actor in all_actors])
        else:
            logger.warning("The MLDataset has not provide the __len__ method, we will iter all "
                           "data to count the number of rows. This should be pretty slowly.")
            it = self.ds.gather_async(batch_ms=0, num_async=self.ds.num_shards())
            it = iter(it)
            num_records = 0
            for pdf in it:
                num_records += pdf.shape[0]
        return num_records


class PrefetchedDataLoader:
    def __init__(self, base_loader, max_size: int = 5):
        self.base_loader = base_loader
        self.max_size = max_size
        self.queue = queue.Queue(maxsize=max_size)
        self.fetcher = None
        self.fetcher_stop = threading.Event()

    def _setup(self):
        if self.fetcher is not None:
            self.fetcher_stop.set()
            if self.queue is not None and not self.queue.empty():
                self.queue.get()
        self.queue = queue.Queue(maxsize=self.max_size)
        self.fetcher = None
        self.fetcher_stop.clear()

        it = iter(self.base_loader)

        def fetch_task():
            while not self.fetcher_stop.is_set():
                try:
                    got_data = next(it)
                    self.queue.put(got_data)
                except StopIteration:
                    self.queue.put(None)
                    break
                except:  # pylint: disable=W0707
                    raise
        self.fetcher = threading.Thread(target=fetch_task)
        self.fetcher.start()

    def __iter__(self):
        self._setup()
        while True:
            fetched_data = self.queue.get()
            if fetched_data is not None:
                yield fetched_data
            else:
                break

    def __len__(self):
        return len(self.base_loader)


def create_data_loader(
        ds: MLDataset,
        world_size: int,
        world_rank: int,
        local_rank: int,
        batch_size: int,
        collate_fn: Callable,
        shuffle: bool,
        shuffle_seed: int = None,
        prefer_node: str = None,
        prefetch: bool = False):
    """
    Create DataLoader from a MLDataset
    :param ds: the MLDataset
    :param world_size: the world_size of distributed model training
    :param world_rank: create the DataLoader for the given world_rank
    :param local_rank: the node local rank
    :param batch_size: the batch_size of the DtaLoader
    :param collate_fn: the collate_fn that create tensors from a pandas DataFrame
    :param shuffle: whether shuffle each batch of data
    :param shuffle_seed: the shuffle seed
    :param prefer_node: the prefer node for create the MLDataset actor
    :param prefetch: prefetch the data of DataLoader with one thread
    :return: a pytorch DataLoader
    """
    num_shards = ds.num_shards()
    assert num_shards % world_size == 0,\
        f"The number shards of MLDataset({ds}) should be a multiple of world_size({world_size})"
    multiplier = num_shards // world_size

    selected_ds = None
    if prefer_node is not None:
        # get all actors
        # there should be only one actor_set because of select_shards() is not allowed
        # after union()

        def location_check(actor):
            address = ray.actors(actor._actor_id.hex())["Address"]["IPAddress"]
            return address == prefer_node

        actors = ds.actor_sets[0].actors
        actor_indexes = [i for i, actor in enumerate(actors) if location_check(actor)]
        if len(actor_indexes) % multiplier == 0:
            selected_ds = None
            logger.info(f"We could not find enough shard actor in prefer node({prefer_node}), "
                        f"fail back to normal select_shards().")
        else:
            shard_ids = actor_indexes[local_rank: local_rank + multiplier]
            selected_ds = ds.select_shards(shard_ids)

    if selected_ds is None:
        shard_ids = []
        i = world_rank
        step = world_size
        while i < num_shards:
            shard_ids.append(i)
            i += step
        selected_ds = ds.select_shards(shard_ids)

    selected_ds = selected_ds.batch(batch_size)
    torch_ds = TorchMLDataset(selected_ds, collate_fn, shuffle, shuffle_seed)
    data_loader = torch.utils.data.DataLoader(dataset=torch_ds,
                                              batch_size=None,
                                              batch_sampler=None,
                                              shuffle=False,
                                              num_workers=0,
                                              collate_fn=None,
                                              pin_memory=False,
                                              drop_last=False,
                                              sampler=None)
    if prefetch:
        data_loader = PrefetchedDataLoader(data_loader)
    return data_loader
