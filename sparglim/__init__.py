#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

"""Top-level package for sparglim."""

__author__ = "wh1isper"
__email__ = "9573586@qq.com"
__version__ = "0.0.1a0"


try:  # noqa
    import findspark  # noqa

    findspark.init()  # noqa
except ImportError:  # noqa
    pass  # noqa
