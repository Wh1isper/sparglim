import os
from datetime import date, datetime

import pytest
from pyspark.sql import Row

from sparglim.config import ConfigBuilder

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


def test_merge(config_builder: ConfigBuilder):
    prev = config_builder._config.copy()
    to_merge = {"a": "b"}
    config_builder._merge_config(to_merge)
    assert config_builder._config == {**prev, **to_merge}

    to_cover = {"a": "c"}
    config_builder._merge_config(to_cover)
    assert config_builder._config == {**prev, **to_cover}


def test_env(config_builder: ConfigBuilder, monkeypatch):
    monkeypatch.setenv("SPAGLIM_APP_NAME", "testapp")
    config_builder = config_builder.clear()
    assert config_builder._config["spark.app.name"] == "testapp"


def test_create(config_builder: ConfigBuilder):
    spark = config_builder.get_or_create()
    config_builder.get_or_create() == spark
    verify_spark(spark, config_builder._config)

    config_builder = config_builder.clear()
    spark = config_builder.get_or_create()
    verify_spark(spark, config_builder._config)


def test_s3(config_builder: ConfigBuilder):
    config_builder.config_s3()
    spark = config_builder.get_or_create()
    verify_spark(spark, config_builder._config)


if __name__ == "__main__":
    pytest.main(["-vvv", _HERE])
