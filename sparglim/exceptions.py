#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License


class UnconfigurableError(Exception):
    ...


class DaemonError(Exception):
    ...


class AlreadyConfiguredError(UnconfigurableError):
    ...
