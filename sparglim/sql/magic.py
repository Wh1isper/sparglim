#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause

from typing import List, Optional

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from pyspark.sql import SparkSession

from sparglim.config import ConfigBuilder
from sparglim.log import logger


@magics_class
class SparkMagic(Magics):
    # TODO: visualize spark dataframe
    def __init__(self, shell=None, **kwargs):
        super().__init__(shell=shell, **kwargs)
        # TODO: Auto config for local/k8s/connect-client
        self.builder = ConfigBuilder().config(
            {
                "spark.sql.repl.eagerEval.enabled": "true",
            }
        )

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
