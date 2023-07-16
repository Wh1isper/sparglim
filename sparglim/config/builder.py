#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

from __future__ import annotations

import os
from collections import UserDict
from functools import wraps
from typing import Any, Dict, Iterable, Literal, Optional, Tuple, Union

from kubernetes.config.incluster_config import SERVICE_TOKEN_FILENAME
from pyspark import SparkConf
from pyspark.sql import SparkSession

from sparglim.config.k8s import INCLUSTER, get_k8s_config, get_namespace
from sparglim.exceptions import UnconfigurableError
from sparglim.log import logger
from sparglim.utils import Singleton, get_host_ip

ConfigEnvMapper = Dict[str, Tuple[Union[Iterable, str], Optional[str]]]


class Config(UserDict):
    def __init__(self, dict=None, /, **kwargs):
        self.master_configured: bool = False
        super().__init__(dict, **kwargs)

    def __setitem__(self, key: Any, item: Any) -> None:
        will_config_master = key in ["spark.master", "spark.remote"]
        if self.master_configured and will_config_master:
            raise UnconfigurableError("Spark master/remote already configured, try clear() first")
        if will_config_master:
            self.master_configured = True

        return super().__setitem__(key, item)

    def pretty_format(self) -> str:
        lines = ["{"]
        for k, v in self.items():
            if ("key" in k.lower() or "secret" in k.lower()) and "file" not in k.lower():
                v = "******"
            lines.append(f'  "{k}":"{v}"')
        lines += ["}"]
        return "\n".join(lines)

    def __repr__(self) -> str:
        return self.pretty_format()


class ConfigBuilder(metaclass=Singleton):
    _basic = {
        "spark.app.name": ("SPAGLIM_APP_NAME", "Sparglim"),
        "spark.submit.deployMode": ("SPAGLIM_DEPLOY_MODE", "client"),
        "spark.scheduler.mode": ("SPARGLIM_SCHEDULER_MODE", "FAIR"),
    }
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
    #  FIXME: Does k8s inject more info into pod?
    _k8s = {
        # May convert by k8s config file(if exsits)
        "spark.master": ("SPARGLIM_MASTER", "k8s://https://kubernetes.default.svc"),
        "spark.kubernetes.namespace": ("SPARGLIM_K8S_NAMESPACE", None),
        # set SPARGLIM_K8S_IMAGE for image:tag
        # or set SPARGLIM_K8S_IMAGE_TAG for wh1isper/spark-executor:tag
        "spark.kubernetes.container.image": (
            "SPARGLIM_K8S_IMAGE",
            f"wh1isper/spark-executor:{os.getenv('SPARGLIM_K8S_IMAGE_TAG', '3.4.1')}",
        ),
        "spark.kubernetes.container.image.pullSecrets": ("SPARGLIM_K8S_IMAGE_PULL_SECRETS", None),
        "spark.kubernetes.container.image.pullPolicy": (
            "SPARGLIM_K8S_IMAGE_PULL_POLICY",
            "IfNotPresent",
        ),
        "spark.executor.instances": ("SPARK_EXECUTOR_NUMS", "3"),
        # Expend list from "a,b" -> [a,b], then map them to {label.a: true, label.b: true}
        "spark.kubernetes.executor.label.list": (
            "SPARGLIM_K8S_EXECUTOR_LABEL_LIST",
            "sparglim-executor",
        ),
        "spark.kubernetes.executor.annotation.list": (
            "SPARGLIM_K8S_EXECUTOR_ANNOTATION_LIST",
            "sparglim-executor",
        ),
        # INCLUSTER, work with k8s filedRef.fieldPath, see example
        # TODO: There is no example yet...
        "spark.driver.host": ("SPARGLIM_DRIVER_HOST", None),
        "spark.driver.bindAddress": ("SPARGLIM_DRIVER_BINDADDRESS", "0.0.0.0"),
        "spark.kubernetes.driver.pod.name": ("SPARGLIM_DRIVER_POD_NAME", None),
        # Authenticate will auto config by k8s config file
    }

    def __init__(self) -> None:
        self.default_config_mapper: ConfigEnvMapper = {
            **self._basic,
            **self._s3,
        }

        self._config: Config
        self.initialize()

        self.default_master_config = self.config_local
        self._spark: Optional[SparkSession] = None

    def initialize(self) -> None:
        self._config: Config = Config(self._config_from_env(self.default_config_mapper))
        logger.debug(f"Initialized config: {self._config}")

    @property
    def master_configured(self) -> bool:
        return self._config.master_configured

    @property
    def spark_config(self) -> SparkConf:
        config = []
        for k, v in self._config.items():
            config.append((k, v))

        return SparkConf().setAll(config)

    def _config_from_env(self, mapper: ConfigEnvMapper) -> Dict[str, Any]:
        config = dict()
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

    def config_k8s(
        self,
        custom_config: Optional[Dict[str, Any]] = None,
        *,
        k8s_config_path: Optional[str] = None,
    ) -> ConfigBuilder:
        if not custom_config:
            custom_config = dict()
        env_config = self._config_from_env(self._k8s)
        if not env_config.get("spark.kubernetes.namespace"):
            env_config["spark.kubernetes.namespace"] = get_namespace()
        if INCLUSTER:
            env_config.setdefault(
                "spark.kubernetes.authenticate.oauthTokenFile", SERVICE_TOKEN_FILENAME
            )
        else:
            env_config.setdefault("spark.driver.host", get_host_ip())

        to_remove_keys = []
        extracted_config = dict()
        for k, v in env_config.items():
            if not k.endswith(".list"):
                continue
            prefix = ".".join(k.split(".")[:-1])
            for item in v.split(","):
                extracted_config[f"{prefix}.{item}"] = "true"
            to_remove_keys.append(k)
        for k in to_remove_keys:
            env_config.pop(k)
        env_config.update(**extracted_config)

        try:
            url, _, ca, key_file, cert_file = get_k8s_config(k8s_config_path)
        except Exception as e:
            logger.exception(e)
            raise UnconfigurableError("Fail to load k8s config")

        master = f"k8s://{url}"
        k8s_config = {
            "spark.master": master,
            "spark.kubernetes.authenticate.caCertFile": ca,
            "spark.kubernetes.authenticate.clientKeyFile": key_file,
            "spark.kubernetes.authenticate.clientCertFile": cert_file,
        }

        self.config(
            {
                **env_config,
                **k8s_config,
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
        if not custom_config:
            custom_config = dict()
        if mode == "local":
            self.config_local(custom_config)
        elif mode == "k8s":
            self.config_k8s(custom_config)
        else:
            raise UnconfigurableError(f"Unknown mode: {mode}")

        self.config(self._config_from_env(self._connect_server))
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


if __name__ == "__main__":
    config = ConfigBuilder()
    config.config_k8s()
    config.get_or_create()
    input()
