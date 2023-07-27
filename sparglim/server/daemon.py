#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import List, Optional

import psutil

from sparglim.config.configer import SparkEnvConfiger
from sparglim.exceptions import DaemonError, UnconfigurableError
from sparglim.log import logger
from sparglim.server.tailer import Tailer
from sparglim.utils import get_scala_version, get_spark_version, port_is_used


class Daemon:
    # This daemon is designed for spark connect server in the frontend
    # Run the connect server in a container and monitor its state, if it is down, restart it
    # Stop the daemon when the KeyboardInterrupt is raised
    # This will not start duplicate connect server, unless using different root_dir and port
    ENV_MASTER_MODE = "SPARGLIM_SERVER_MODE"
    DEFAULT_MODE = "local"
    SPARK_IDENT_STRING = "sparglim-connect-server"  # SPARK_IDENT_STRING

    def __init__(
        self,
        mode: Optional[str] = None,
        root_dir: str = "./",
        *,
        k8s_config_path: Optional[str] = None,
    ):
        self.mode = mode or os.getenv(self.ENV_MASTER_MODE, self.DEFAULT_MODE)

        self.root_dir = Path(root_dir)

        self.log_dir = self.root_dir / "logs"  # SPARK_LOG_DIR
        self.pid_dir = self.root_dir / "pids"  # SPARK_PID_DIR
        self.log_glob_format = f"spark-{self.SPARK_IDENT_STRING}-*.out"
        self.pid_glob_format = f"spark-{self.SPARK_IDENT_STRING}-*.pid"
        self.daemon_pid_file = self.pid_dir / f"daemon.pid"

        self.spark_cmd_dir = Path(f"{os.getenv('SPARK_HOME')}/sbin")
        self.start_cmd_path = self.spark_cmd_dir / "start-connect-server.sh"
        if not self.start_cmd_path.exists():
            raise UnconfigurableError(
                f"{self.start_cmd_path.name} not found in $SPARK_HOME/sbin, check your SPARK_HOME"
            )

        SPARK_VERSION = os.getenv("SPARK_VERSION", get_spark_version())
        SCALA_VERSION = os.getenv("SCALA_VERSION", get_scala_version())
        add_connect_package = os.getenv("SPARGLIM_SERVER_CONNECT_PACKAGES", "false") in [
            "true",
            "True",
        ]
        self.daemon_package = []
        if add_connect_package and (SPARK_VERSION and SCALA_VERSION):
            # Specify the spark-connect package or use the default one
            self.daemon_package.append(
                f"org.apache.spark:spark-connect_{SCALA_VERSION}:{SPARK_VERSION}"
            )
        self.builder = SparkEnvConfiger().config_connect_server(
            self.mode, k8s_config_path=k8s_config_path
        )

        self._tailer: Optional[Tailer] = None
        self._daemon: Optional[threading.Thread] = None
        self.is_stopping: bool = False

    @property
    def config(self) -> List[str]:
        config = []
        for k, v in self.builder.get_all().items():
            config.append("--conf")
            config.append(f"{k}={v}")
        return config

    @property
    def packages(self) -> List[str]:
        packages = []
        for p in self.daemon_package:
            packages.append("--packages")
            packages.append(p)

        return packages

    @property
    def pid_file(self) -> Optional[Path]:
        for pid_file in self.pid_dir.glob(self.pid_glob_format):
            return pid_file
        return None

    @property
    def log_file(self) -> Optional[Path]:
        for log_file in self.log_dir.glob(self.log_glob_format):
            return log_file
        return None

    @property
    def pid(self) -> Optional[int]:
        if not self.pid_file:
            return None
        return int(self.pid_file.read_text())  # type: ignore

    @property
    def port(self) -> int:
        return int(self.builder.get_all().get("spark.connect.grpc.binding.port", "15002"))

    def start_and_daemon(self):
        # Start spark connect server and daemon it
        # If it is already running, just daemon it
        if not self.pid or not psutil.pid_exists(self.pid):
            self._start()
        if self.daemon_pid_file.exists():
            d_pid = self.daemon_pid_file.read_text()
            if psutil.pid_exists(int(d_pid)):
                logger.warning(f"Daemon is already running, pid: {d_pid}, exit")
                return
        else:
            self.daemon_pid_file.write_text(str(os.getpid()))
        self._launch_daemon()
        self._wait_exit()

    def _start(self):
        self._rotate_log()
        logger.info(f"Start connect server, work dir: {self.root_dir.absolute()}")
        if port_is_used(self.port):
            logger.error(f"Port {self.port} is already in use")
            raise UnconfigurableError(f"Port {self.port} is already in use")
        env = os.environ.copy()
        env["SPARK_LOG_DIR"] = self.log_dir.absolute().as_posix()
        env["SPARK_PID_DIR"] = self.pid_dir.absolute().as_posix()
        env["SPARK_IDENT_STRING"] = self.SPARK_IDENT_STRING
        # We rotate log by ourselves
        env["SPARK_LOG_MAX_FILES"] = "1"
        cmd = [self.start_cmd_path.absolute().as_posix()] + self.config + self.packages
        p = subprocess.Popen(
            cmd,
            cwd=self.root_dir.absolute().as_posix(),
            env=env,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        p.wait()

        assert self.pid
        assert self.log_file

        logger.info(
            # type: ignore
            f"Connect server started, pid: {self.pid}, logs {self.log_file.absolute().as_posix()}"
        )

    def _wait_exit(self):
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        signal.pause()
        self.daemon_pid_file.unlink()

    def _daemon_and_tail(self):
        daemon_pid = self.pid
        logger.debug(f"Daemon pid {daemon_pid}")
        if not daemon_pid:
            raise DaemonError("No pid to daemon")
        self._tailer = Tailer(self.log_file.as_posix(), daemon_pid).start()  # type: ignore

        def _(daemon_process, interval=1):
            while True:
                time.sleep(interval)
                if not self.pid or self.is_stopping:
                    # No pid file or stopping flag, assume server stopped by self.stop()
                    logger.info("Exit daemon as server has been stopped")
                    self._tailer.stop()
                    return

                if daemon_process.pid != self.pid:
                    # Pid changed, others take over
                    logger.warning("pid has changed, exit")
                    self._tailer.stop()
                    return

                if not daemon_process.is_running():
                    logger.warning("Daemon process is not running, try to restart it")
                    # We have pid file and pid in file is the same as daemon_pid, but no process, restart
                    self._start()
                    daemon_process = psutil.Process(self.pid)
                    # Start a new tailer for new pid
                    self._tailer.stop()
                    self._tailer = Tailer(self.log_file.as_posix(), daemon_process.pid).start()

        self._daemon = threading.Thread(target=_, args=(psutil.Process(daemon_pid),))
        self._daemon.start()

    def _rotate_log(self):
        now = datetime.now()
        count = 1
        for log_file in self.log_dir.glob(self.log_glob_format):
            logger.debug(f"Rotate {log_file.as_posix()}")
            log_file.rename(log_file.with_suffix(f".{now}.log.rotate.{count}"))
            count += 1

    def _launch_daemon(self):
        logger.info("Start daemon connect server")
        if not self.pid:
            raise UnconfigurableError("Connect server is not running")
        self._daemon_and_tail()

    def _stop_connect_server(self, pid: int):
        logger.info("Stop connect server")
        try:
            p = psutil.Process(pid)
        except psutil.NoSuchProcess:
            # Already stopped
            pass
        else:
            # We have the process
            try:
                p.terminate()
                logger.info("Wating for connect server to stop")
                p.wait(30)
            except psutil.TimeoutExpired:
                logger.warning("Timeout for terminate connect server, kill it")
                p.kill()

    def stop(self, sig=None, frame=None):
        self.is_stopping = True
        try:
            pid = self.pid
            if pid:
                self._stop_connect_server(pid)
            if self.pid_file:
                self.pid_file.unlink()
            self._rotate_log()
            if self._daemon:
                self._daemon.join()
        finally:
            self._stopping = False


if __name__ == "__main__":
    d = Daemon()

    d.start_and_daemon()
