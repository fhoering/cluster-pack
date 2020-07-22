import hashlib
import json
import logging
import os
import subprocess
import conda_pack

from typing import Dict

from cluster_pack import process


_logger = logging.getLogger(__name__)


def get_conda_env_name(conda_env_path=None, reqs: Dict[str, str] = {}, env_id=None):
    conda_env_contents = open(conda_env_path).read() if conda_env_path else ""
    if reqs:
        for k, v in reqs.items():
            conda_env_contents += k + v
    if env_id:
        conda_env_contents += env_id
    return "cluster-pack-%s" % hashlib.sha1(conda_env_contents.encode("utf-8")).hexdigest()


def get_conda_bin_executable(executable_name):
    """
    Return path to the specified executable, assumed to be discoverable within the 'bin'
    subdirectory of a conda installation.
    """
    # Use CONDA_EXE as per https://github.com/conda/conda/issues/7126
    if "CONDA_EXE" in os.environ:
        conda_bin_dir = os.path.dirname(os.environ["CONDA_EXE"])
        return os.path.join(conda_bin_dir, executable_name)
    return executable_name


def get_or_create_conda_env(project_env_name=None, conda_env_spec=None):
    conda_path = get_conda_bin_executable("conda")
    try:
        process.call([conda_path, "--help"], throw_on_error=False)
    except EnvironmentError:
        raise RuntimeError(f"Could not find Conda executable at {conda_path}.")

    _logger.info(f"search conda envs for {project_env_name}")

    env_names = [os.path.basename(env) for env in _list_envs(conda_path)]
    if project_env_name not in env_names:
        _logger.info(f"Creating conda environment {project_env_name}")
        if conda_env_spec:
            process.call([conda_path, "env", "create", "-n", project_env_name, "--file",
                      conda_env_spec], stream_output=True)
        else:
            process.call(
                [conda_path, "create", "-n", project_env_name, "python"], stream_output=True)

    project_env_path = [env for env in _list_envs(conda_path)
                        if os.path.basename(env) == project_env_name][0]

    _logger.info(f'project env path is {project_env_path}')

    return project_env_path


def _list_envs(conda_path):
    _, stdout, _ = process.exec_cmd([conda_path, "env", "list", "--json"])
    return [env for env in json.loads(stdout)['envs']]
