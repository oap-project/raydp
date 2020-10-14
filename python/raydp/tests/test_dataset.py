import sys

import pandas as pd
import pytest

import raydp
import raydp.parallel as parallel


def test_from_range(ray_cluster):
    ds = parallel.from_range(10, 3)
    assert ds.num_shards() == 3
    assert ds.repeatable()
    assert not ds.repeated()
    results = sorted(ds.take(10))
    assert results == list(range(10))
    assert ds.count() == 10

    # repeat
    ds = ds.repeat()
    assert not ds.repeatable()
    assert ds.repeated()
    results = ds.take(11)
    assert len(results) == 11

    del ds

    # repeated
    ds = parallel.from_range(9, 3, True)
    assert not ds.repeatable()
    assert ds.repeated()
    results = sorted(ds.take(9))
    assert results == list(range(9))


def test_from_items(ray_cluster):
    ds = parallel.from_items(list(range(10)), 3)
    assert ds.num_shards() == 3
    assert ds.repeatable()
    assert not ds.repeated()
    results = sorted(ds.take(10))
    assert results == list(range(10))
    assert ds.count() == 10

    ds = ds.repeat()
    assert not ds.repeatable()
    assert ds.repeated()
    results = ds.take(11)
    assert len(results) == 11

    del ds

    # repeat
    ds = parallel.from_items(list(range(9)), 3, True)
    assert not ds.repeatable()
    assert ds.repeated()
    results = sorted(ds.take(9))
    assert results == list(range(9))


def test_from_spark(ray_cluster):
    spark = raydp.init_spark("test", 1, 1, "500 M")
    df = spark.range(0, 10)
    ds = parallel.from_spark_df(df, 2)
    assert ds.repeatable()
    assert not ds.repeated()
    assert ds.count() == 2  # 2 pandas DataFrame
    shard_0 = ds.get_shard(0)
    df_0 = next(iter(shard_0))
    assert isinstance(df_0, pd.DataFrame)
    shard_1 = ds.get_shard(1)
    df_1 = next(iter(shard_1))
    assert isinstance(df_1, pd.DataFrame)
    assert (len(df_0) + len(df_1)) == 10


def test_normal_transform(ray_cluster):
    ds = parallel.from_range(10, 3)

    # test map
    ds = ds.map(lambda x: x + 1)
    assert ds.count() == 10
    assert ds.take(1)[0] == 1

    # test filter
    ds = ds.filter(lambda x: x > 1)
    assert ds.count() == 9
    assert ds.take(1)[0] == 2

    # test flatmap
    ds = ds.flatmap(lambda x: list(range(x)))
    assert ds.count() == 54
    assert ds.take(1)[0] == 0


def test_get_shard(ray_cluster):
    ds = parallel.from_range(9, 3)
    it = ds.get_shard(0)
    assert it.repeatable()
    assert it.repeatable()
    assert len(it) == 3

    it = it.repeat()
    assert not it.repeatable()
    assert it.repeated()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
