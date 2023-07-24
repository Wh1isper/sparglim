from __future__ import annotations

import subprocess
import sys
from typing import Optional

from sparglim.log import logger


class Tailer:
    def __init__(self, file_path: str, pid: int):
        self.cmd = ["tail", "-f", "--pid", f"{pid}", file_path]
        self._task: Optional[subprocess.Popen] = None

    def start(self) -> Tailer:
        if self._task and self._task.poll() is None:
            logger.info("Tailer is already running")
            return self
        logger.info(f"Start tailer by cmd: {self.cmd}")
        self._task = subprocess.Popen(
            self.cmd,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        return self

    def stop(self):
        logger.debug("Try to stop tailer.")
        self._task.kill()
        self._task.wait()
        logger.debug("Tailer stopped")
