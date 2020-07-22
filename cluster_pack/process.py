
import logging
import subprocess

from typing import List, Tuple


_logger = logging.getLogger(__name__)


def call(cmd: List[str], **kwargs) -> Tuple[int, str, str]:
    _logger.info(" ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    out, err = proc.communicate()
    if proc.returncode != 0:
        _logger.error(out)
        _logger.error(err)
        raise subprocess.CalledProcessError(proc.returncode, cmd)

    _logger.debug(out)
    _logger.debug(err)
    return proc.returncode, out, err
