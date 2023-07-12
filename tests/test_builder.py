#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

import os
from datetime import date, datetime
from typing import Dict

import pytest
from pyspark.sql import Row

from sparglim.config import ConfigBuilder
from sparglim.exceptions import UnconfigurableError

_HERE = os.path.abspath(__file__)


@pytest.fixture
def config_builder():
    c = ConfigBuilder()
    yield c
    c.clear()


def verify_spark(spark, config):
    df = spark.createDataFrame(
        [
            Row(a=1, b=2.0, c="string1", d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
            Row(a=2, b=3.0, c="string2", d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
            Row(a=4, b=5.0, c="string3", d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0)),
        ]
    )
    df.show()

    for k, v in config.items():
        assert spark.conf.get(k) == v


def assert_contain(left: Dict, right: Dict):
    for k, v in right.items():
        assert left[k] == v


def patch_env(config_builder: ConfigBuilder, monkeypatch, mapper: Dict[str, str]) -> ConfigBuilder:
    for k, v in mapper.items():
        monkeypatch.setenv(k, v)
    return config_builder.clear()


@pytest.mark.parametrize("mode", ["local", "k8s", "connect_client", "connect_server"])
def test_deploy_mode(config_builder: ConfigBuilder, mode: str):
    config_builder = config_builder.clear()
    config_mode = getattr(config_builder, f"config_{mode}")
    config_mode()
    assert config_builder.deploy_mode_configured
    with pytest.raises(UnconfigurableError) as e:
        config_mode()


def test_merge(config_builder: ConfigBuilder):
    prev = config_builder._config.copy()
    to_merge = {"a": "b"}
    config_builder._merge_config(to_merge)
    assert config_builder._config == {**prev, **to_merge}

    to_cover = {"a": "c"}
    config_builder._merge_config(to_cover)
    assert config_builder._config == {**prev, **to_cover}


def test_config(config_builder: ConfigBuilder):
    prev = config_builder._config.copy()
    to_merge = {"spark.app.name": "appname"}
    config_builder.config(to_merge)
    assert config_builder._config == {**prev, **to_merge}

    to_cover = {"spark.app.name": "appname2"}
    config_builder.config(to_cover)
    assert config_builder._config == {**prev, **to_cover}


def test_env(config_builder: ConfigBuilder, monkeypatch):
    config_builder = patch_env(config_builder, monkeypatch, {"SPAGLIM_APP_NAME": "testapp"})
    assert config_builder._config["spark.app.name"] == "testapp"


def test_create(config_builder: ConfigBuilder):
    spark = config_builder.get_or_create()
    config_builder.get_or_create() == spark
    verify_spark(spark, config_builder._config)

    old_spark = spark

    config_builder = config_builder.clear()
    spark = config_builder.get_or_create()
    assert old_spark != spark
    verify_spark(spark, config_builder._config)


s3_env_cases = [
    (
        {
            "S3_ACCESS_KEY": "s3-access-key",
            "S3_SECRET_KEY": "s3-secret-key",
            "S3_ENTRY_POINT": "s3-entry-point",
            "S3_ENTRY_POINT_REGION": "s3-entry-point-region",
            "S3_PATH_STYLE_ACCESS": "true",
            "S3_MAGIC_COMMITTER": "true",
        },
        {
            "spark.hadoop.fs.s3a.access.key": "s3-access-key",
            "spark.hadoop.fs.s3a.secret.key": "s3-secret-key",
            "spark.hadoop.fs.s3a.endpoint": "s3-entry-point",
            "spark.hadoop.fs.s3a.endpoint.region": "s3-entry-point-region",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled": "true",
        },
    ),
    (
        {
            "AWS_ACCESS_KEY_ID": "aws-access-key",
            "AWS_SECRET_ACCESS_KEY": "aws-secret-key",
            "AWS_DEFAULT_REGION": "aws-entry-point-region",
        },
        {
            "spark.hadoop.fs.s3a.access.key": "aws-access-key",
            "spark.hadoop.fs.s3a.secret.key": "aws-secret-key",
            "spark.hadoop.fs.s3a.endpoint.region": "aws-entry-point-region",
        },
    ),
]


@pytest.mark.parametrize("env_mapper, expect_mapper", s3_env_cases)
def test_s3(
    config_builder: ConfigBuilder,
    monkeypatch,
    env_mapper: Dict[str, str],
    expect_mapper: Dict[str, str],
):
    config_builder = patch_env(config_builder, monkeypatch, env_mapper)
    config_builder.config_s3()
    assert_contain(config_builder._config, expect_mapper)

    convert_mapper = {
        "spark.hadoop.fs.s3a.access.key": "convert-access-key",
        "spark.hadoop.fs.s3a.secret.key": "convert-secret-key",
    }
    config_builder.config_s3(**convert_mapper)
    assert_contain(config_builder._config, {**expect_mapper, **convert_mapper})


if __name__ == "__main__":
    pytest.main(["-vv", "-s", __file__])
