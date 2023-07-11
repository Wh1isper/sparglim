from .magic import ConfigBuilder, SparkMagic


def load_ipython_extension(ipython):
    ipython.register_magics(SparkMagic)


def unload_ipython_extension(ipython):
    ConfigBuilder().clear()
