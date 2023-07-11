#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

import findspark

findspark.init()

from .builder import ConfigBuilder

__all__ = ["ConfigBuilder"]
