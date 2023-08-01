#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

from .magic import SparkMagic


def load_ipython_extension(ipython):

    code = """
print("Creating SparkSession as `spark`")
from sparglim.config.builder import ConfigBuilder
# ConfigBuilder is a singleton, and already configured by SparkMagic()
spark = ConfigBuilder().get_or_create()
"""
    ipython.register_magics(SparkMagic)  # SparkMagic already has been initialized
    ipython.run_cell(code)


def unload_ipython_extension(ipython):
    SparkMagic.clear()
