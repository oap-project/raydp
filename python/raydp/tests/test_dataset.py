import sys

import pytest

import raydp
import raydp.parallel as parallel


def test_from_range():
    ds = parallel.from_range(10, 3)
    assert ds.num_shards() == 3
    assert ds.repeatable()
    assert not ds.repeated()
    results = ds.take(10)
    assert results == list(range(10))
    assert ds.count() == 10

    ds = ds.repeat()
    assert not ds.repeatable()
    assert ds.repeated()
    results = ds.take(11)
    assert results == (list(range(10)) + [0])

    del ds

    # repeat
    ds = parallel.from_range(10, 3, True)
    assert not ds.repeatable()
    assert ds.repeated()
    results = ds.take(10)
    assert results == list(range(10))


def test_from_items():
    ds = parallel.from_items(list(range(10)), 3)
    assert ds.num_shards() == 3
    assert ds.repeatable()
    assert not ds.repeated()
    results = ds.take(10)
    assert results == list(range(10))
    assert ds.count() == 10

    ds = ds.repeat()
    assert not ds.repeatable()
    assert ds.repeated()
    results = ds.take(11)
    assert results == (list(range(10)) + [0])

    del ds

    # repeat
    ds = parallel.from_items(list(range(10)), 3, True)
    assert not ds.repeatable()
    assert ds.repeated()
    results = ds.take(10)
    assert results == list(range(10))


def test_from_spark(ray_cluster):
    spark = raydp.init_spark("test", 1, 1, "500 M")
    df = spark.range(0, 10)
    ds = parallel.from_spark_df(df, 3)
    assert ds.count() == 10


def test_normal_transform():
    ds = parallel.from_range(10, 3)

    # test map
    ds = ds.map(lambda x: x + 1)
    assert ds.count() == 10
    assert ds.take(0) == 1

    # test filter
    ds = ds.filter(lambda x: x > 1)
    assert ds.count() == 9
    assert ds.take(0) == 2

    # test flatmap
    ds = ds.flatmap(lambda x: list(range(x)))
    assert ds.count() == 10
    assert ds.take(0) == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
