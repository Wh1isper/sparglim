#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License
import os
import socket
from pathlib import Path
from typing import Dict, Optional


def port_is_used(port: int) -> bool:
    port = int(port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("0.0.0.0", port))
        return False
    except OSError:
        return True


def get_spark_version() -> Optional[str]:
    spark_home = os.getenv("SPARK_HOME")
    if not spark_home:
        return get_pyspark_version()
    jars_dir: Path = Path(spark_home) / "jars"
    for p in jars_dir.glob("spark-core*"):
        # spark-core_2.12-3.4.1.jar -> 3.4.1
        return p.stem.split("_")[-1].split("-")[-1]
    return None


def get_pyspark_version() -> Optional[str]:
    pyspark_version = None
    try:
        import pyspark

        pyspark_version = pyspark.__version__
    except ImportError:
        pass
    return pyspark_version


def get_scala_version() -> Optional[str]:
    spark_home = os.getenv("SPARK_HOME")
    if not spark_home:
        return None
    jars_dir: Path = Path(spark_home) / "jars"
    for p in jars_dir.glob("spark-core*"):
        # spark-core_2.12-3.4.1.jar -> 2.12
        return p.stem.split("_")[-1].split("-")[0]
    return None


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def get_host_ip():
    # Get ip via network connection
    s = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        if s:
            s.close()
    return ip


def set_if_not_none(config_dict: Dict, k, v):
    if v is not None:
        config_dict[k] = v
