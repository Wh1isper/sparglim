#  Copyright (c) 2023 Wh1isper
#  Licensed under the BSD 3-Clause License
import os
import signal
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import psutil

from sparglim.config import SparkEnvConfiger
from sparglim.exceptions import DaemonError, UnconfigurableError
from sparglim.log import logger
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
        mode: str = "local",
        root_dir: str = "./",
        port: str = "15002",
        *,
        k8s_config_path: Optional[str] = None,
    ):
        self.mode = mode or os.getenv(self.ENV_MASTER_MODE, self.DEFAULT_MODE)
        self.daemon_config = {"spark.connect.grpc.binding.port": port}

        self.root_dir = Path(root_dir)

        self.log_dir = self.root_dir / "logs"  # SPARK_LOG_DIR
        self.pid_dir = self.root_dir / "pids"  # SPARK_PID_DIR
        self.log_glob_format = f"spark-{self.SPARK_IDENT_STRING}-*.out"
        self.pid_glob_format = f"spark-{self.SPARK_IDENT_STRING}-*.pid"

        self.spark_cmd_dir = Path(f"{os.getenv('SPARK_HOME')}/sbin")
        self.start_cmd_path = self.spark_cmd_dir / "start-connect-server.sh"
        if not self.start_cmd_path.exists():
            raise UnconfigurableError(
                f"{self.start_cmd_path.name} not found in $SPARK_HOME/sbin, check your SPARK_HOME"
            )

        SPARK_VERSION = os.getenv("SPARK_VERSION", get_spark_version())
        SCALA_VERSION = os.getenv("SCALA_VERSION", get_scala_version())
        self.daemon_package = []
        if SPARK_VERSION and SCALA_VERSION:
            # Specify the spark-connect package or use the default one
            self.daemon_package.append(
                f"org.apache.spark:spark-connect_{SCALA_VERSION}:{SPARK_VERSION}"
            )
        self.builder = SparkEnvConfiger().config_connect_server(
            self.mode, self.daemon_config, k8s_config_path=k8s_config_path
        )

        # TODO: They may be thread?
        self.daemoner = None
        self.tailer = None

    @property
    def config(self) -> List[str]:
        return [f"--conf {k}={v}" for k, v in self.builder.get_all().items()]

    @property
    def packages(self) -> List[str]:
        return [f"--packages {p}" for p in self.daemon_package]

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
        return int(self.pid_file.read_text())

    @property
    def port(self) -> str:
        return self.daemon_config["spark.connect.grpc.binding.port"]

    def start_or_daemon(self):
        # Start spark connect server and daemon it
        # If it is already running, just daemon it
        if not self.pid or psutil.pid_exists(self.pid):
            self._start()
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
        logger.info(
            f"Connect server started, pid: {self.pid}, logs {self.log_file.absolute().as_posix()}"
        )

        assert self.pid

    def _wait_exit(self):
        signal.signal(signal.SIGINT, self.stop)
        signal.pause()

    def _daemon_pid(self):
        # TODO: Daemon {pid}, if it is not running, restart it
        daemon_pid = self.pid
        if not daemon_pid:
            raise DaemonError("No pid to daemon")

        if daemon_pid != self.pid:
            # Pid changed, exit
            logger.warning("Pid has changed, exit ")

    def _rotate_log(self):
        now = datetime.now()
        count = 1
        for log_file in self.log_dir.glob(self.log_glob_format):
            logger.debug(f"Rotate {log_file.as_posix()}")
            log_file.rename(log_file.with_suffix(f".{count}.rotate.log.{now}"))
            count += 1

    def _tail_log(self):
        # TODO: tail log to stdout, may start a thread to do this
        #       What will happen if the log file is deleted or rotated?
        #       Or daemon restart server
        pass

    def _launch_daemon(self):
        logger.info("Start daemon connect server")
        if not self.pid:
            raise UnconfigurableError("Connect server is not running")
        logger.debug(f"Daemon pid {self.pid}")

        # TODO: may change imple
        self._daemon_pid()
        self._tail_log()

    def stop(self, sig=None, frame=None):
        if not self.pid:
            logger.warning("No connect server to stop")

        logger.info("Stop connect server")
        try:
            p = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            # Already stopped
            pass
        else:
            # We have the process
            try:
                p.terminate()
                logger.info("Wating for connect server to stop")
                p.wait(10)
            except psutil.TimeoutExpired:
                logger.warning("Timeout for terminate connect server, kill it")
                p.kill()

        if self.pid_file:
            self.pid_file.unlink()
        self._rotate_log()

        sys.exit(0)


if __name__ == "__main__":
    d = Daemon()
    d.start_or_daemon()
