#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

from __future__ import annotations

import os
from functools import wraps
from typing import Any, Dict, Literal, Optional, Tuple

from pyspark import SparkConf
from pyspark.sql import SparkSession

from sparglim.exceptions import UnconfigurableError
from sparglim.log import logger
from sparglim.utils import Singleton


class ConfigBuilder(metaclass=Singleton):
    _basic = {
        "spark.app.name": ("SPAGLIM_APP_NAME", "Sparglim"),
        "spark.submit.deployMode": ("SPAGLIM_DEPLOY_MODE", "client"),
        "spark.scheduler.mode": ("SPARGLIM_SCHEDULER_MODE", "FAIR"),
    }
    # FIXME: S3 secret(and any other) should not be printed
    _s3 = {
        "spark.hadoop.fs.s3a.access.key": (["S3_ACCESS_KEY", "AWS_ACCESS_KEY_ID"], None),
        "spark.hadoop.fs.s3a.secret.key": (["S3_SECRET_KEY", "AWS_SECRET_ACCESS_KEY"], None),
        "spark.hadoop.fs.s3a.endpoint": ("S3_ENTRY_POINT", None),
        "spark.hadoop.fs.s3a.endpoint.region": (
            ["S3_ENTRY_POINT_REGION", "AWS_DEFAULT_REGION"],
            None,
        ),
        "spark.hadoop.fs.s3a.path.style.access": ("S3_PATH_STYLE_ACCESS", None),
        "spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled": ("S3_MAGIC_COMMITTER", None),
    }
    _local = {
        "spark.master": ("SPARGLIM_MASTER", "local[*]"),
        "spark.driver.memory": ("SPARGLIM_LOCAL_MEMORY", "512m"),
    }
    # TODO: config for connect-server mode
    _connect_client = {
        "spark.remote": ("SPARGLIM_REMOTE", "sc://localhost"),
    }
    _connect_server = {}
    # TODO: config for k8s
    #       verify volumn mount/secret etc.
    #       Does k8s inject authorization into pod?
    _k8s = {
        "spark.master": ("SPARGLIM_MASTER", "k8s://https://kubernetes.default.svc"),
    }

    def __init__(self) -> None:
        self.master_configured: bool
        self.default_config: Dict[str, Any]
        self._config: Dict[str, Any]
        self.initialize()
        self.default_master_config = self.config_local
        self._spark: Optional[SparkSession] = None

    def initialize(self) -> None:
        self.master_configured: bool = False
        self.default_config = {
            **self._basic,
            **self._s3,
        }
        self._config: Dict[str, Any] = self._config_from_env(self.default_config)
        logger.debug("Initialized config: {self._config}")

    @property
    def spark_config(self) -> SparkConf:
        config = []
        for k, v in self._config.items():
            config.append((k, v))

        return SparkConf().setAll(config)

    def _config_from_env(self, mapper: Dict[str, Tuple[str, str]]) -> Dict[str, Any]:
        config = {}
        for k, (envs, default) in mapper.items():
            if isinstance(envs, str):
                envs = [envs]
            for env in envs:
                v = os.getenv(env)
                if v:
                    break
            v = v or default

            if v:
                config[k] = v

        return config

    def clear(self) -> ConfigBuilder:
        logger.info("Reinitialize config and stop SparkSession")
        self.initialize()
        if self._spark:
            self._spark.stop()
            self._spark = None
        return self

    def _merge_config(self, c: Dict[str, Any]) -> None:
        logger.debug(f"Merge config: {c}")
        self._config.update(**c)
        logger.debug(f"Current config: {self._config}")

    def config(self, c: Dict[str, Any]) -> ConfigBuilder:
        will_config_master = c.get("spark.master") or c.get("spark.remote")
        if self.master_configured and will_config_master:
            raise UnconfigurableError("Spark master already configured, try clear() first")
        if will_config_master:
            self.master_configured = True

        self._merge_config(c)
        return self

    def config_s3(self, custom_config: Optional[Dict[str, Any]] = None) -> ConfigBuilder:
        if not custom_config:
            custom_config = dict()
        self.config(
            {
                **self._config_from_env(self._s3),
                **custom_config,
            }
        )
        return self

    def config_local(self, custom_config: Optional[Dict[str, Any]] = None) -> ConfigBuilder:
        if not custom_config:
            custom_config = dict()
        self.config(
            {
                **self._config_from_env(self._local),
                **custom_config,
            }
        )
        return self

    def config_k8s(self, custom_config: Optional[Dict[str, Any]] = None) -> ConfigBuilder:
        if not custom_config:
            custom_config = dict()
        self.config(
            {
                **self._config_from_env(self._k8s),
                **custom_config,
            }
        )
        return self

    def config_connect_client(
        self, custom_config: Optional[Dict[str, Any]] = None
    ) -> ConfigBuilder:
        if not custom_config:
            custom_config = dict()
        self.config(
            {
                **self._config_from_env(self._connect_client),
                **custom_config,
            }
        )
        return self

    def config_connect_server(
        self,
        mode: Literal["local", "k8s"] = "local",
        custom_config: Optional[Dict[str, Any]] = None,
    ) -> ConfigBuilder:
        if mode == "local":
            self.config_local(custom_config)
        elif mode == "k8s":
            self.config_k8s(custom_config)
        else:
            raise UnconfigurableError(f"Unknown mode: {mode}")

        self._merge_config(self._config_from_env(self._connect_server))
        return self

    def create(self) -> SparkSession:
        logger.info("Create SparkSession")
        return SparkSession.builder.config(conf=self.spark_config).getOrCreate()

    def get_or_create(self) -> SparkSession:
        if self._spark:
            return self._spark

        if not self.master_configured:
            self.default_master_config()

        self._spark = self.create()
        return self._spark
