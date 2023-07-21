import findspark  # noqa

findspark.init()  # noqa


from typing import Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession

from sparglim.config.configer import SparkEnvConfiger
from sparglim.log import logger
from sparglim.utils import Singleton


class ConfigBuilder(SparkEnvConfiger, metaclass=Singleton):
    def __init__(self):
        super().__init__()
        self._spark: Optional[SparkSession] = None

    @property
    def spark_config(self) -> SparkConf:
        config = []
        for k, v in self.get_all().items():
            config.append((k, v))

        return SparkConf().setAll(config)

    def _create(self) -> SparkSession:
        logger.info("Create SparkSession")
        return SparkSession.builder.config(conf=self.spark_config).getOrCreate()

    def get_or_create(self) -> SparkSession:
        if self._spark:
            return self._spark

        if not self.master_configured:
            self.default_master_config()

        self._spark = self._create()
        return self._spark
