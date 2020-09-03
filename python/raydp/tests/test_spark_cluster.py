import sys

import pytest

import raydp.spark.context as context


def test_spark_standalone(ray_cluster):
    spark = context.init_spark("test", 1, 1, "500 M", resource_manager="standalone")
    result = spark.range(0, 10).count()
    assert result == 10
    context.stop_spark()


def test_spark_native(ray_cluster):
    spark = context.init_spark("test", 1, 1, "500 M", resource_manager="ray")
    result = spark.range(0, 10).count()
    assert result == 10
    context.stop_spark()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
