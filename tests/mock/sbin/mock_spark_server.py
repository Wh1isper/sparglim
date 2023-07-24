#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

import os
import random
import signal
import string
import sys
import threading
import time
from pathlib import Path

log_dir = Path(os.environ["SPARK_LOG_DIR"])
log_dir.mkdir(parents=True, exist_ok=True)
spark_ident_string = os.environ["SPARK_IDENT_STRING"]
log_file = log_dir / f"spark-{spark_ident_string}-mock-server.out"


def _append_text(self, target: str) -> None:
    with self.open("a") as f:
        f.write(target)


Path.append_text = _append_text


def random_str():
    return "".join(random.choices(string.ascii_letters, k=10)) + "\n"


def start_write_random_str():
    while True:
        log_file.append_text(random_str())
        time.sleep(1)


def stop(sig, frame):
    log_file.append_text("Mock Spark Server Stopped\n")
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    threading.Thread(target=start_write_random_str, daemon=True).start()
    signal.pause()


if __name__ == "__main__":
    main()
