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

import math
import sys

# https://spark.apache.org/docs/latest/api/python/migration_guide/koalas_to_pyspark.html
# import databricks.koalas as ks
import pyspark.pandas as ps
import pyspark
import pytest

import raydp.utils as utils


def test_df_type_check(spark_session):
    spark_df = spark_session.range(0, 10)
    koalas_df = ps.range(0, 10)
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

    koalas_df = ps.range(0, 10)
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

    koalas_df = ps.range(0, 10)
    splits = utils.random_split(koalas_df, [0.7, 0.3])
    assert isinstance(splits[0], ps.DataFrame)
    assert isinstance(splits[1], ps.DataFrame)
    assert len(splits) == 2


def test_memory_size_parser():
    upper_units = ["", "K", "M", "G", "T"]
    expected = [10 * math.pow(2, 10 * p) for p in range(len(upper_units))]

    # upper without B
    values = [f"10{unit}" for unit in upper_units]
    parsed = [utils.parse_memory_size(v) for v in values]
    assert parsed == expected
    # lower without B
    values = [f"10{unit.lower()}" for unit in upper_units]
    parsed = [utils.parse_memory_size(v) for v in values]
    assert parsed == expected
    # upper blank without B
    values = [f"10 {unit}" for unit in upper_units]
    parsed = [utils.parse_memory_size(v) for v in values]
    assert parsed == expected
    # upper two blanks without B
    values = [f"10  {unit}" for unit in upper_units]
    parsed = [utils.parse_memory_size(v) for v in values]
    assert parsed == expected

    upper_units = ["B", "KB", "MB", "GB", "TB"]
    # upper with B
    values = [f"10{unit}" for unit in upper_units]
    parsed = [utils.parse_memory_size(v) for v in values]
    assert parsed == expected
    # lower with B
    values = [f"10{unit.lower()}" for unit in upper_units]
    parsed = [utils.parse_memory_size(v) for v in values]
    assert parsed == expected
    # upper blank with B
    values = [f"10 {unit}" for unit in upper_units]
    parsed = [utils.parse_memory_size(v) for v in values]
    assert parsed == expected
    # upper two blanks with B
    values = [f"10  {unit}" for unit in upper_units]
    parsed = [utils.parse_memory_size(v) for v in values]
    assert parsed == expected


def test_divide_blocks():
    blocks = [5, 1, 2, 3, 5, 6, 2, 1, 2]
    world_size = 3

    def get_num_records(sub_blocks):
        nums = 0
        for index, num in sub_blocks:
            assert num <= blocks[index]
            nums += num
        return nums

    divided_blocks = utils.divide_blocks(blocks, world_size, False)
    assert len(divided_blocks) == 3

    blocks_0 = get_num_records(divided_blocks[0])
    blocks_1 = get_num_records(divided_blocks[1])
    blocks_2 = get_num_records(divided_blocks[2])
    assert blocks_0 == blocks_1 == blocks_2

    divided_blocks = utils.divide_blocks(blocks, world_size, True)
    assert len(divided_blocks) == 3

    blocks_0 = get_num_records(divided_blocks[0])
    blocks_1 = get_num_records(divided_blocks[1])
    blocks_2 = get_num_records(divided_blocks[2])
    assert blocks_0 == blocks_1 == blocks_2

    blocks = [5, 1, 2, 3, 5, 6, 2, 2, 2]
    world_size = 3

    divided_blocks = utils.divide_blocks(blocks, world_size, False)
    assert len(divided_blocks) == 3

    blocks_0 = get_num_records(divided_blocks[0])
    blocks_1 = get_num_records(divided_blocks[1])
    blocks_2 = get_num_records(divided_blocks[2])
    assert blocks_0 == blocks_1 == blocks_2

    divided_blocks = utils.divide_blocks(blocks, world_size, True)
    assert len(divided_blocks) == 3

    blocks_0 = get_num_records(divided_blocks[0])
    blocks_1 = get_num_records(divided_blocks[1])
    blocks_2 = get_num_records(divided_blocks[2])
    assert blocks_0 == blocks_1 == blocks_2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
