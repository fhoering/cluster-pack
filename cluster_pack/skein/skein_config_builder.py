import cloudpickle
import logging
import os
import skein
import time

from typing import NamedTuple, Tuple, Callable, Dict, List, Optional, Any

from cluster_pack import packaging, uploader

logger = logging.getLogger(__name__)


class SkeinConfig(NamedTuple):
    script: str
    files: Dict[str, str]
    env: Dict[str, str]


def build_with_func(
        func: Callable,
        args: List[Any] = [],
        package_path: Optional[str] = None,
        additional_files: Optional[List[str]] = None,
        tmp_dir: str = packaging._get_tmp_dir(),
        forward_kerberos_ticket: bool = False
) -> SkeinConfig:
    """Build the skein config from provided a function

    The function is serialized and shipped to the container

    Returns
    -------
    SkeinConfig
    """
    function_path = f'{tmp_dir}/function.dat'
    val_to_serialize = {
        "func": func,
        "args": args
    }
    with open(function_path, "wb") as fd:
        cloudpickle.dump(val_to_serialize, fd)

    if additional_files:
        additional_files.append(function_path)
    else:
        additional_files = [function_path]

    return build(
        'cluster_pack.skein._execute_fun',
        ['function.dat'],
        package_path,
        additional_files,
        tmp_dir,
        forward_kerberos_ticket)


def build(
        module_name: str,
        args: List[Any] = [],
        package_path: Optional[str] = None,
        additional_files: Optional[List[str]] = None,
        tmp_dir: str = packaging._get_tmp_dir(),
        forward_kerberos_ticket: bool = False
) -> SkeinConfig:
    """Build the skein config for a module to execute

    Returns
    -------
    SkeinConfig

    """
    if not package_path:
        package_path, _ = uploader.upload_env()

    script = _get_script(
        package_path,
        module_name,
        args,
        forward_kerberos_ticket)

    files, env = _get_files_and_env(
        package_path,
        additional_files,
        tmp_dir,
        forward_kerberos_ticket)

    return SkeinConfig(script, files, env)


def _get_script(
        package_path: str,
        module_name: str,
        args: List[Any] = [],
        forward_kerberos_ticket: bool = False
) -> str:
    python_bin = f"./{os.path.basename(package_path)}" if package_path.endswith(
        '.pex') else f"./{os.path.basename(package_path)}/bin/python"

    launch_options = "-m" if not module_name.endswith(".py") else ""
    launch_args = " ".join(args)

    unset_hadoop_token = "unset HADOOP_TOKEN_FILE_LOCATION" if forward_kerberos_ticket else None

    script = f'''
                {unset_hadoop_token}
                {python_bin} {launch_options} {module_name} {launch_args}
              '''

    return script


def _get_files_and_env(
        package_path: str,
        additional_files: Optional[List[str]] = None,
        tmp_dir: str = packaging._get_tmp_dir(),
        forward_kerberos_ticket: bool = False
) -> Tuple[Dict[str, str], Dict[str, str]]:

    env = {
        "SKEIN_CONFIG": "./.skein",
        "GIT_PYTHON_REFRESH": "quiet",
        "PEX_ROOT": "./.pex",
        "PYTHONPATH": "."
    }

    files_to_upload = [package_path]
    if additional_files:
        files_to_upload = files_to_upload + additional_files

    if forward_kerberos_ticket:
        krb5_ticket_path = get_kerberos_ticket_path()
        if krb5_ticket_path:
            env['KRB5CCNAME'] = f'FILE:{os.path.basename(krb5_ticket_path)}'
            files_to_upload = files_to_upload + [krb5_ticket_path]

    dict_files_to_upload = {os.path.basename(path): path
                            for path in files_to_upload}

    editable_requirements = packaging.get_editable_requirements()

    editable_packages = {name: packaging.zip_path(path, False) for name, path in
                         editable_requirements.items()}
    dict_files_to_upload.update(editable_packages)

    editable_packages_index = f"{tmp_dir}/{packaging.EDITABLE_PACKAGES_INDEX}"

    try:
        os.remove(editable_packages_index)
    except OSError:
        pass

    with open(editable_packages_index, "w+") as file:
        for repo in editable_requirements.keys():
            file.write(repo + "\n")
    dict_files_to_upload[
        packaging.EDITABLE_PACKAGES_INDEX
    ] = editable_packages_index

    return dict_files_to_upload, env


def get_kerberos_ticket_path() -> Optional[str]:
    try:
        if "KRB5CCNAME" not in os.environ:
            filepath = f"/tmp/krb5cc_{os.getuid()}"
        else:
            filepath = os.environ["KRB5CCNAME"]
            if filepath.startswith('FILE:'):
                return filepath[5:]
        return filepath
    except KeyError:
        return None
