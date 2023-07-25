#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

import os
from datetime import date, datetime
from typing import Dict

import pytest
from pyspark.sql import Row

from sparglim.config.builder import ConfigBuilder
from sparglim.exceptions import UnconfigurableError


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


def assert_contain(left, right):
    for k, v in right.items():
        if v == None:
            assert k not in left
            continue
        assert k in left
        assert left[k] == v


def patch_env(config_builder: ConfigBuilder, monkeypatch, mapper: Dict[str, str]) -> ConfigBuilder:
    for k, v in mapper.items():
        monkeypatch.setenv(k, v)
    return config_builder.clear()


@pytest.mark.parametrize("mode", ["local", "k8s", "connect_client", "connect_server"])
def test_deploy_mode(config_builder: ConfigBuilder, mode: str, k8s_config_path):
    config_builder = config_builder.clear()
    config_mode = getattr(config_builder, f"config_{mode}")
    if mode == "k8s":
        config_mode(k8s_config_path=k8s_config_path)
    else:
        config_mode()
    if mode != "connect_server":
        assert config_builder.master_configured

    if mode in ["local", "k8s"]:
        with pytest.raises(UnconfigurableError) as e:
            config_mode()


def test_merge(config_builder: ConfigBuilder):
    prev = config_builder.get_all().copy()
    to_merge = {"a": "b"}
    config_builder._merge_config(to_merge)
    assert config_builder.get_all() == {**prev, **to_merge}

    to_cover = {"a": "c"}
    config_builder._merge_config(to_cover)
    assert config_builder.get_all() == {**prev, **to_cover}


def test_config(config_builder: ConfigBuilder):
    prev = config_builder.get_all().copy()
    to_merge = {"spark.app.name": "appname"}
    config_builder.config(to_merge)
    assert config_builder.get_all() == {**prev, **to_merge}

    to_cover = {"spark.app.name": "appname2"}
    config_builder.config(to_cover)
    assert config_builder.get_all() == {**prev, **to_cover}


def test_env(config_builder: ConfigBuilder, monkeypatch):
    config_builder = patch_env(config_builder, monkeypatch, {"SPAGLIM_APP_NAME": "testapp"})
    assert config_builder.get_all()["spark.app.name"] == "testapp"


def test_create(config_builder: ConfigBuilder):
    spark = config_builder.get_or_create()
    config_builder.get_or_create() == spark
    verify_spark(spark, config_builder.get_all())

    old_spark = spark

    config_builder = config_builder.clear()
    spark = config_builder.get_or_create()
    assert old_spark != spark
    verify_spark(spark, config_builder.get_all())


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
    assert_contain(config_builder.get_all(), expect_mapper)

    convert_mapper = {
        "spark.hadoop.fs.s3a.access.key": "convert-access-key",
        "spark.hadoop.fs.s3a.secret.key": "convert-secret-key",
    }
    config_builder.config_s3(convert_mapper)
    assert_contain(config_builder.get_all(), {**expect_mapper, **convert_mapper})


# TODO:
# Mock env as incluster
k8s_env_mapper = [
    (
        {
            "SPARGLIM_K8S_NAMESPACE": "namespace",
            "SPARGLIM_K8S_IMAGE": "image",
            "SPARK_EXECUTOR_NUMS": "4",
            "SPARGLIM_K8S_EXECUTOR_LABEL_LIST": "a,b",
            "SPARGLIM_K8S_EXECUTOR_ANNOTATION_LIST": "a,b",
            "SPARGLIM_DRIVER_HOST": "headless-sparglim",
            "SPARGLIM_DRIVER_POD_NAME": "some-spark-app",
            "SPARGLIM_DRIVER_BINDADDRESS": "192.168.1.1",
            "SPARGLIM_K8S_EXECUTOR_REQUEST_CORES": "1",
            "SPARGLIM_K8S_EXECUTOR_LIMIT_CORES": "4",
            "SPARGLIM_EXECUTOR_REQUEST_MEMORY": "1024m",
            "SPARGLIM_EXECUTOR_LIMIT_MEMORY": "2048m",
            "SPARGLIM_K8S_GPU_AMOUNT": "1",
        },
        {
            "spark.kubernetes.namespace": "namespace",
            "spark.kubernetes.container.image": "image",
            "spark.executor.instances": "4",
            "spark.kubernetes.executor.label.a": "true",
            "spark.kubernetes.executor.label.b": "true",
            "spark.kubernetes.executor.annotation.a": "true",
            "spark.kubernetes.executor.annotation.b": "true",
            "spark.driver.host": "headless-sparglim",
            "spark.kubernetes.driver.pod.name": "some-spark-app",
            "spark.driver.bindAddress": "192.168.1.1",
            "spark.kubernetes.executor.cores": "1",
            "spark.kubernetes.executor.limit.cores": "4",
            "spark.executor.memory": "1024m",
            "spark.executor.memoryOverhead": "2048m",
            "spark.executor.resource.gpu.vendor": "nvidia.com",
            "spark.executor.resource.gpu.discoveryScript": "/opt/spark/examples/src/main/scripts/getGpusResources.sh",
            "spark.executor.resource.gpu.amount": "1",
        },
    ),
    (
        {
            # Test default value
        },
        {
            "spark.kubernetes.namespace": "sparglim",
            "spark.kubernetes.executor.label.sparglim-executor": "true",
            "spark.kubernetes.executor.annotation.sparglim-executor": "true",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.executor.memory": "512m",
            "spark.executor.resource.gpu.vendor": None,
            "spark.executor.resource.gpu.discoveryScript": None,
        },
    ),
]


@pytest.mark.parametrize("env_mapper, expect_mapper", k8s_env_mapper)
def test_k8s(
    config_builder: ConfigBuilder,
    monkeypatch,
    k8s_config_path,
    env_mapper: Dict[str, str],
    expect_mapper: Dict[str, str],
):
    config_builder = patch_env(config_builder, monkeypatch, env_mapper)
    config_builder.config_k8s(k8s_config_path=k8s_config_path)
    assert_contain(config_builder.get_all(), expect_mapper)


def test_k8s_no_config(config_builder: ConfigBuilder):
    with pytest.raises(UnconfigurableError) as e:
        config_builder.config_k8s(k8s_config_path="NOT-EXSIT-FILE")


connect_client_mapper = [
    ({"SPARGLIM_REMOTE": "sc://localhost:9999"}, {"spark.remote": "sc://localhost:9999"}),
    (
        {
            # Test default value
        },
        {"spark.remote": "sc://localhost:15002"},
    ),
]


@pytest.mark.parametrize("env_mapper, expect_mapper", connect_client_mapper)
def test_connect_client(
    config_builder: ConfigBuilder,
    monkeypatch,
    env_mapper: Dict[str, str],
    expect_mapper: Dict[str, str],
):
    config_builder = patch_env(config_builder, monkeypatch, env_mapper)
    config_builder.config_connect_client()
    assert_contain(config_builder.get_all(), expect_mapper)


connect_server_mapper = [
    (
        {
            "SPARGLIM_CONNECT_SERVER_PORT": "9999",
            "SPARGLIM_CONNECT_GRPC_ARROW_MAXBS": "12345",
            "SPARGLIM_CONNECT_GRPC_MAXIM": "67890",
        },
        {
            "spark.connect.grpc.binding.port": "9999",
            "spark.connect.grpc.arrow.maxBatchSize": "12345",
            "spark.connect.grpc.maxInboundMessageSize": "67890",
        },
    ),
    (
        {
            # Test default value
        },
        {},
    ),
]


@pytest.mark.parametrize("env_mapper, expect_mapper", connect_server_mapper)
def test_connect_server(
    config_builder: ConfigBuilder,
    monkeypatch,
    env_mapper: Dict[str, str],
    expect_mapper: Dict[str, str],
):
    config_builder = patch_env(config_builder, monkeypatch, env_mapper)
    config_builder.config_connect_server()
    assert_contain(config_builder.get_all(), expect_mapper)


if __name__ == "__main__":
    pytest.main(["-vv", "-s", __file__])
