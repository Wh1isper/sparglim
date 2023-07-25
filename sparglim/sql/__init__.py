#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

from .magic import SparkMagic


def load_ipython_extension(ipython):
    ipython.register_magics(SparkMagic)


def unload_ipython_extension(ipython):
    SparkMagic.clear()
