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

import numpy as np
import pandas as pd
import pytest
import sys
import torch

from raydp.spark.torch.dataset import BLOCK_SIZE_BIT, BlockSetSampler, RayDataset, PandasDataset


class DummyRayDataset(RayDataset):
    def __init__(self, data):
        self.data = data

    def _resolve_with_indices(self, indices, plasma_store_socket_name):
        pass

    def __getitem__(self, index):
        block_index = index >> BLOCK_SIZE_BIT
        block_inner_index = (block_index << BLOCK_SIZE_BIT) ^ index
        if block_index != self._previous_block_index:
            self._previous_block_index = block_index
            df = self._block_set[block_index]
            self._feature_df = df[self._feature_columns].values
            self._label_df = df[self._label_column].values
        return self._get_next(block_inner_index, self._feature_df, self._label_df)

    def __len__(self):
        """Get the total size"""
        return sum([len(d) for d in self.data])

    def block_sizes(self):
        """Get the block sizes"""
        return [len(d) for d in self.data]


def test_balanced_blockset_sampler():
    tmp = range(0, 20)
    data = [tmp[0: 5], tmp[5: 10], tmp[10: 15], tmp[15: 20]]
    dataset = DummyRayDataset(data)
    assert len(dataset) == 20
    assert dataset.block_sizes() == [5, 5, 5, 5]

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=0, shuffle=False, init_lazy=False)
    assert sampler.block_indices == [0, 2]
    assert len(sampler) == 10
    block_index = 0
    results = [((block_index << BLOCK_SIZE_BIT) | i) for i in range(5)]
    block_index = 2
    results += [((block_index << BLOCK_SIZE_BIT) | i) for i in range(5)]
    assert results == list(iter(sampler))

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=0, shuffle=True, init_lazy=False)
    assert len(sampler.block_indices) == 2
    assert len(sampler) == 10
    assert results != list(iter(sampler))
    sorted_sampler_result = sorted(list(iter(sampler)))

    def check(results):
        block_index = results[0] >> BLOCK_SIZE_BIT
        previous = 0
        for item in results:
            index = item >> BLOCK_SIZE_BIT
            inner_index = (index << BLOCK_SIZE_BIT) ^ item
            assert index == block_index
            assert inner_index == previous
            previous += 1

    check(sorted_sampler_result[0: 5])
    check(sorted_sampler_result[5: 10])


def test_unbalanced_blockset_sampler():
    tmp = range(0, 15)
    data = [tmp[0: 4], tmp[4: 12], tmp[12: 15]]
    dataset = DummyRayDataset(data)
    assert len(dataset) == 15
    assert dataset.block_sizes() == [4, 8, 3]

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=0, shuffle=False, init_lazy=False)
    assert sampler.block_indices == sorted([0, 2, 1])
    assert len(sampler) == 8
    block_index = 0
    results = [((block_index << BLOCK_SIZE_BIT) | i) for i in range(4)]
    block_index = 2
    results += [((block_index << BLOCK_SIZE_BIT) | i) for i in range(3)]
    block_index = 1
    results += [((block_index << BLOCK_SIZE_BIT) | i) for i in range(1)]

    assert results == list(iter(sampler))

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=1, shuffle=False, init_lazy=False)
    assert sampler.block_indices == [1]
    assert len(sampler) == 8
    block_index = 1
    results = [((block_index << BLOCK_SIZE_BIT) | i) for i in range(8)]

    assert results == list(iter(sampler))

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=0, shuffle=True, init_lazy=False)
    assert len(sampler) == 8

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=1, shuffle=True, init_lazy=False)
    assert len(sampler) == 8


def test_normal_pandas_dataset():
    normal_df = pd.DataFrame({"feature": np.arange(2, 7),
                              "label": range(5)})

    dataset = PandasDataset(normal_df,
                            feature_columns=["feature"],
                            label_column="label")
    assert len(dataset) == 5
    feature, label = dataset[0]
    assert feature.item() == 2
    assert label.item() == 0
    feature, label = dataset[4]
    assert feature.item() == 6
    assert label.item() == 4

    # with given type
    dataset = PandasDataset(normal_df,
                            feature_columns=["feature"],
                            feature_types=[torch.float],
                            label_column="label",
                            label_type=torch.int16)

    feature, label = dataset[0]
    assert feature.dtype == torch.float
    assert feature.item() == 2.0
    assert label.dtype == torch.int16
    assert label.item() == 0

    feature, label = dataset[4]
    assert feature.dtype == torch.float
    assert feature.item() == 6
    assert label.dtype == torch.int16
    assert label.item() == 4


def test_complicated_pandas_dataset():
    df = pd.DataFrame({"feature1": [[1, 2, 3, 4],
                                    [2, 3, 4, 5],
                                    [3, 4, 5, 6]],
                       "feature2": [6, 7, 8],
                       "label": range(3)})

    dataset = PandasDataset(df,
                            feature_columns=["feature1", "feature2"],
                            feature_shapes=[[2, 2], 0],
                            label_column="label")
    assert len(dataset) == 3
    feature1, feature2, label = dataset[0]
    assert feature1.shape == torch.Size((2, 2))
    assert feature1.numpy().tolist() == [[1, 2], [3, 4]]
    assert feature2.shape == torch.Size([])
    assert feature2.item() == 6
    assert label.item() == 0

    df = pd.DataFrame({"feature1": [(1, 2, 3, 4),
                                    (2, 3, 4, 5),
                                    (3, 4, 5, 6)],
                       "feature2": [6, 7, 8],
                       "label": range(3)})

    dataset = PandasDataset(df,
                            feature_columns=["feature1", "feature2"],
                            feature_shapes=[[2, 2], 0],
                            label_column="label")
    assert len(dataset) == 3
    feature1, feature2, label = dataset[0]
    assert feature1.shape == torch.Size((2, 2))
    assert feature1.numpy().tolist() == [[1, 2], [3, 4]]
    assert feature2.shape == torch.Size([])
    assert feature2.item() == 6
    assert label.item() == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
