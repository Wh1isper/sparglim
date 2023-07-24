#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

import os
import subprocess
from pathlib import Path

_HERE = os.path.dirname(__file__)
mock_spark_server = os.path.join(_HERE, "mock_spark_server.py")


def start_mock_server():
    p = subprocess.Popen(
        ["python", mock_spark_server],
        env=os.environ.copy(),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    pid_dir = Path(os.environ["SPARK_PID_DIR"])
    spark_ident_string = os.environ["SPARK_IDENT_STRING"]
    pid_dir.mkdir(parents=True, exist_ok=True)
    pid_file = pid_dir / f"spark-{spark_ident_string}-mock-server.pid"
    pid_file.write_text(str(p.pid))

    log_dir = Path(os.environ["SPARK_LOG_DIR"])
    log_dir.mkdir(parents=True, exist_ok=True)
    spark_ident_string = os.environ["SPARK_IDENT_STRING"]
    log_file = log_dir / f"spark-{spark_ident_string}-mock-server.out"
    log_file.touch(exist_ok=True)


if __name__ == "__main__":
    print("Starting Mock Spark Server...")
    start_mock_server()
