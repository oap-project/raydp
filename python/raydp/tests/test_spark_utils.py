import databricks.koalas as ks

import pyspark
import pytest

import raydp.spark.utils as utils

import sys


def test_df_type_check(spark_session):
    spark_df = spark_session.range(0, 10)
    koalas_df = ks.range(0, 10)
    assert utils.df_type_check(spark_df)
    assert utils.df_type_check(koalas_df)

    other_df = "df"
    error_msg = (f"The type: {type(other_df)} is not supported, only support " +
                 "pyspark.sql.DataFrame and databricks.koalas.DataFrame")
    with pytest.raises(Exception) as exinfo:
        utils.df_type_check(other_df)
    assert str(exinfo.value) == error_msg


def test_convert_to_spark(spark_session):
    spark_df = spark_session.range(0, 10)
    converted, is_spark_df = utils.convert_to_spark(spark_df)
    assert is_spark_df
    assert spark_df is converted

    koalas_df = ks.range(0, 10)
    converted, is_spark_df = utils.convert_to_spark(koalas_df)
    assert not is_spark_df
    assert isinstance(converted, pyspark.sql.DataFrame)
    assert converted.count() == 10

    other_df = "df"
    error_msg = (f"The type: {type(other_df)} is not supported, only support " +
                 "pyspark.sql.DataFrame and databricks.koalas.DataFrame")
    with pytest.raises(Exception) as exinfo:
        utils.df_type_check(other_df)
    assert str(exinfo.value) == error_msg


def test_random_split(spark_session):
    spark_df = spark_session.range(0, 10)
    splits = utils.random_split(spark_df, [0.7, 0.3])
    assert len(splits) == 2

    koalas_df = ks.range(0, 10)
    splits = utils.random_split(koalas_df, [0.7, 0.3])
    assert isinstance(splits[0], ks.DataFrame)
    assert isinstance(splits[1], ks.DataFrame)
    assert len(splits) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
