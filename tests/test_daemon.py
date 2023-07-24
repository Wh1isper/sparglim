#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License

import os
import shutil
import signal
import threading
import time
from pathlib import Path

import pytest

from sparglim.server.daemon import Daemon

_HERE = os.path.dirname(__file__)
mock_dir = Path(_HERE) / "mock"


@pytest.fixture
def spark_home():
    yield mock_dir.absolute().as_posix()


@pytest.fixture
def tmppath(tmpdir):
    tmppath = Path(tmpdir)
    yield tmppath
    pid_dir = tmppath / "pids"
    pid_file = pid_dir / "spark-sparglim-connect-server-mock-server.pid"
    if pid_file.exists():
        pid = int(pid_file.read_text())
        try:
            os.kill(pid, 9)
        except Exception:
            pass
    shutil.rmtree(tmppath.as_posix())


@pytest.mark.timeout(35)
def test_daemon(monkeypatch, spark_home, tmppath):
    monkeypatch.setenv("SPARK_HOME", spark_home)

    def _interrupt():
        time.sleep(2)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=_interrupt, daemon=True).start()
    daemon = Daemon(root_dir=tmppath)
    daemon.start_and_daemon()


if __name__ == "__main__":
    pytest.main(["-vv", "-s", __file__])
