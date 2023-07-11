from __future__ import annotations

import os
from functools import wraps
from typing import Any, Dict, Literal, Optional, Tuple

from pyspark import SparkConf
from pyspark.sql import SparkSession

from sparglim.exceptions import UnconfigurableError
from sparglim.log import logger
from sparglim.utils import Singleton


def config_deploy_mode(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.deploy_mode_configured:
            raise UnconfigurableError("Deploy mode already configured.")
        self.deploy_mode_configured = True
        return func(self, *args, **kwargs)

    return wrapper


class ConfigBuilder(metaclass=Singleton):
    _basic = {
        "spark.app.name": ("SPAGLIM_APP_NAME", "Sparglim"),
        "spark.submit.deployMode": ("SPAGLIM_DEPLOY_MODE", "client"),
        "spark.scheduler.mode": ("SPARGLIM_SCHEDULER_MODE", "FAIR"),
    }
    _s3 = {
        "spark.hadoop.fs.s3a.access.key": ("S3_ACCESS_KEY", None),
        "spark.hadoop.fs.s3a.secret.key": ("S3_SECRET_KEY", None),
        "spark.hadoop.fs.s3a.endpoint": ("S3_ENTRY_POINT", None),
        "spark.hadoop.fs.s3a.endpoint.region": ("S3_ENTRY_POINT_REGION", None),
        "spark.hadoop.fs.s3a.path.style.access": ("S3_PATH_STYLE_ACCESS", None),
        "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled": ("S3_MAGIC_COMMITTER", None),
    }
    _local = {
        "spark.master": ("SPARGLIM_MASTER", "local[*]"),
        "spark.driver.memory": ("SPARGLIM_LOCAL_MEMORY", "512m"),
    }
    # TODO: config for connect-server mode
    _connect_client = {}
    _connect_server = {}
    # TODO: config for k8s
    #       verify volumn mount/secret etc.
    #       Does k8s inject authorization into pod?
    _k8s = {}

    def __init__(self) -> None:
        self.deploy_mode_configured: bool = False
        self._config: Dict[str, Any] = self._config_from_env(self._basic)
        self._spark: Optional[SparkSession] = None

    @property
    def spark_config(self) -> SparkConf:
        config = []
        for k, v in self._config.items():
            config.append((k, v))

        return SparkConf().setAll(config)

    def _config_from_env(self, mapper: Dict[str, Tuple[str, str]]) -> Dict[str, Any]:
        config = {}
        for k, (env, default) in mapper.items():
            v = os.getenv(env, default)
            if v:
                config[k] = v

        return config

    def clear(self) -> ConfigBuilder:
        self._config: Dict[str, Any] = self._config_from_env(self._basic)
        if self._spark:
            self._spark.stop()
            self._spark = None
        return self

    def _merge_config(self, c: Dict[str, Any]) -> None:
        logger.debug(f"Merge config: {c}")
        self._config.update(**c)
        logger.debug(f"Current config: {self._config}")

    def config_s3(self, **custom_config: Dict[str, Any]) -> ConfigBuilder:
        self._merge_config(self._config_from_env(self._s3))
        self._merge_config(custom_config)
        return self

    @config_deploy_mode
    def config_local(self, **custom_config: Dict[str, Any]) -> ConfigBuilder:
        self._merge_config(self._config_from_env(self._local))
        self._merge_config(custom_config)
        return self

    @config_deploy_mode
    def config_k8s(self, **custom_config: Dict[str, Any]) -> ConfigBuilder:
        self._merge_config(self._config_from_env(self._k8s))
        self._merge_config(custom_config)
        return self

    @config_deploy_mode
    def config_connect_client(self, **custom_config: Dict[str, Any]) -> ConfigBuilder:
        self._merge_config(self._config_from_env(self._connect_client))
        self._merge_config(custom_config)
        return self

    def config_connect_server(
        self, mode: Literal["local", "k8s"] = "local", **custom_config: Dict[str, Any]
    ) -> ConfigBuilder:
        if mode == "local":
            self._merge_config(self._config_from_env(self._local))
        elif mode == "k8s":
            self._merge_config(self._config_from_env(self._k8s))
        else:
            raise ValueError(f"Unknown mode: {mode}")

        self._merge_config(self._config_from_env(self._connect_server))
        self._merge_config(custom_config)
        return self

    def _create(self) -> SparkSession:
        return SparkSession.builder.config(conf=self.spark_config).getOrCreate()

    def get_or_create(self) -> SparkSession:
        if self._spark:
            return self._spark

        if not self.deploy_mode_configured:
            self.config_local()

        self._spark = self._create()
        return self._spark
