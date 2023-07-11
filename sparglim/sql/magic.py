#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License
from typing import List

from IPython.core.magic import Magics, cell_magic, line_magic, magics_class
from pyspark.sql import SparkSession

from sparglim.config import ConfigBuilder


@magics_class
class SparkMagic(Magics):
    # TODO: visualize spark dataframe
    def __init__(self, shell=None, **kwargs):
        super().__init__(shell=shell, **kwargs)
        # TODO: Auto config for local/k8s/connect-client
        self.builder = ConfigBuilder()

    @property
    def spark(self) -> SparkSession:
        return self.builder.get_or_create()

    def format_sql(self, *sql_lines: List[str]):
        lines = ""
        for line in sql_lines:
            lines += line.strip() + "\n"

        return lines.strip()

    def _execute(self, sql):
        return self.spark.sql(sql)

    @line_magic
    def sql(self, line: str):
        return self._execute(self.format_sql([line]))

    @cell_magic
    def csql(self, line: str, cell: str):
        return self._execute(self.format_sql([line, cell]))

    def clear(self):
        self.builder.clear()
