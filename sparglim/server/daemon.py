#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

import os

import findspark

# TODO: A daemon is needed for connect server
from pyspark.version import __version__

findspark.init()
_HERE = os.path.dirname(os.path.abspath(__file__))


LOG_DIR = os.path.join(_HERE, "./tmp/sparglim/logs")
SPARK_PID_DIR = os.path.join(_HERE, "./tmp/sparglim/pids")


SPARK_VERSION = os.getenv("SPARK_VERSION", __version__)


start_cmd = f"{os.getenv('SPARK_HOME')}/sbin/start-connect-server.sh "
stop_cmd = f"{os.getenv('SPARK_HOME')}/sbin/stop-connect-server.sh "

if __name__ == "__main__":
    pass
