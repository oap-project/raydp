import pytest
import sys

from raydp.spark.torch.dataset import BLOCK_SIZE_BIT, BlockSetSampler, RayDataset


class DummyRayDataset(RayDataset):
    def __init__(self, data):
        self.data = data

    def _resolve_with_indices(self, indices):
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

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=0, shuffle=False)
    assert sampler.block_indices == [0, 2]
    assert len(sampler) == 10
    block_index = 0
    results = [((block_index << BLOCK_SIZE_BIT) | i) for i in range(5)]
    block_index = 2
    results += [((block_index << BLOCK_SIZE_BIT) | i) for i in range(5)]
    assert results == list(iter(sampler))

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=0, shuffle=True)
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

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=0, shuffle=False)
    assert sampler.block_indices == [0, 2, 1]
    assert len(sampler) == 8
    block_index = 0
    results = [((block_index << BLOCK_SIZE_BIT) | i) for i in range(4)]
    block_index = 2
    results += [((block_index << BLOCK_SIZE_BIT) | i) for i in range(3)]
    block_index = 1
    results += [((block_index << BLOCK_SIZE_BIT) | i) for i in range(1)]

    assert results == list(iter(sampler))

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=1, shuffle=False)
    assert sampler.block_indices == [1]
    assert len(sampler) == 8
    block_index = 1
    results = [((block_index << BLOCK_SIZE_BIT) | i) for i in range(8)]

    assert results == list(iter(sampler))

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=0, shuffle=True)
    assert len(sampler) == 8

    sampler = BlockSetSampler(dataset, num_replicas=2, rank=1, shuffle=True)
    assert len(sampler) == 8


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))

