#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License
import socket
from typing import Dict


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


def get_host_ip():
    # Get ip via network connection
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip


def set_if_not_none(config_dict: Dict, k, v):
    if v is not None:
        config_dict[k] = v


if __name__ == "__main__":
    print(get_host_ip())
