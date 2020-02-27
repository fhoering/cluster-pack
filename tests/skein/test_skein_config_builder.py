import contextlib
import json
import os
import pyarrow
import subprocess
from subprocess import check_output
import sys
import shutil
import tempfile
from unittest import mock
import zipfile

import pytest

from pex.pex_info import PexInfo

from cluster_pack.skein import skein_config_builder


def test_build():
    skein_config = skein_config_builder.build(
        "mymodule",
        args=[],
        package_path="mypackage.pex")

    assert "./mypackage.pex -m mymodule" in skein_config.script
    assert "mypackage.pex" in skein_config.files
    assert "PEX_ROOT" in skein_config.env


def test_build_forward_kerberos_ticket():
    skein_config = skein_config_builder.build(
        "mymodule",
        args=[],
        package_path="mypackage.pex",
        forward_kerberos_ticket=True)

    print(skein_config)

    # assert "./mypackage.pex -m mymodule" in skein_config.script
    # assert "mypackage.pex" in skein_config.files
    # assert "PEX_ROOT" in skein_config.env
