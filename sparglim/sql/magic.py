#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause
import os
from typing import List, Optional

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from pyspark.sql import SparkSession

from sparglim.config.builder import ConfigBuilder
from sparglim.exceptions import UnconfigurableError
from sparglim.log import logger


@magics_class
class SparkMagic(Magics):
    """
    # Enable magic

        ```ipython
        %load_ext sparglim.sql
        ```

    # Example:

        Init a table

        ```python
        from datetime import datetime, date
        from pyspark.sql import Row

        from sparglim.config.builder import ConfigBuilder
        spark = ConfigBuilder().get_or_create()


        df = spark.createDataFrame([
            Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
            Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
            Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
        ])
        df.show()
        df.createOrReplaceTempView("tb")
        ```

        Query it

        ```ipython
        %sql SELECT * FROM tb
        ```

        or
        ```ipython
        %%sql SELECT
        *
        FROM
        tb
        ```
    """

    ENV_MASTER_MODE = "SPARGLIM_SQL_MODE"
    DEFAULT_MODE = "local"

    # TODO: visualize spark dataframe
    def __init__(self, shell=None, **kwargs):
        if "k8s_config_path" in kwargs:
            k8s_config_path = kwargs.pop("k8s_config_path")
        else:
            k8s_config_path = None
        super().__init__(shell=shell, **kwargs)
        self.builder = ConfigBuilder()
        if self.builder.spark:
            logger.info("SparkSession already exists, skip init")
            self.builder.set_runtime_conf("spark.sql.repl.eagerEval.enabled", "true")
            return

        self.builder.config(
            {
                "spark.sql.repl.eagerEval.enabled": "true",
            }
        )

        self.mode: str = os.getenv(self.ENV_MASTER_MODE, self.DEFAULT_MODE)

        if self.mode == "local":
            self.builder.config_local()
        elif self.mode == "connect_client":
            self.builder.config_connect_client()
        elif self.mode == "k8s":
            self.builder.config_k8s(k8s_config_path=k8s_config_path)
        else:
            raise UnconfigurableError(f"Unconfigurable mode {self.mode}")

    @property
    def spark(self) -> SparkSession:
        return self.builder.get_or_create()

    def format_sql(self, sql_lines: List[Optional[str]]):
        # format sql to one line
        lines = ""
        for line in sql_lines:
            line = line or ""
            lines += line.strip() + " "

        return lines.strip()

    def _execute_one(self, sql=str):
        if not sql:
            return
        logger.debug(f"Execute Spark SQL: {sql}")
        return self.spark.sql(sql)

    def _execute(self, sql=str):
        r = None
        for s in sql.split(";"):
            if s.strip():  # ignore empty sql
                r = self._execute_one(s.strip())
        return r

    @line_magic
    @cell_magic
    def sql(self, line: Optional[str] = None, cell: Optional[str] = None):
        return self._execute(self.format_sql([line, cell]))

    @classmethod
    def clear(self):
        # ConfigBuilder is Singleton
        ConfigBuilder().clear()
