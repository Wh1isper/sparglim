import findspark

findspark.init()

from .builder import ConfigBuilder

__all__ = ["ConfigBuilder"]
