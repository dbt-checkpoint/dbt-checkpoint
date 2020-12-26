import json
import subprocess
from argparse import ArgumentParser
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Generator
from typing import List
from typing import NoReturn
from typing import Optional
from typing import Sequence


class CalledProcessError(RuntimeError):
    pass


class ManifestOpenError(RuntimeError):
    pass


def cmd_output(*cmd: str, expected_code: Optional[int] = 0, **kwargs: Any) -> str:
    kwargs.setdefault("stdout", subprocess.PIPE)
    kwargs.setdefault("stderr", subprocess.PIPE)
    proc = subprocess.Popen(cmd, **kwargs)
    stdout, stderr = proc.communicate()
    stdout = stdout.decode()
    if expected_code is not None and proc.returncode != expected_code:
        raise CalledProcessError(
            cmd,
            expected_code,
            proc.returncode,
            stdout,
            stderr,
        )
    return stdout


def cmd_realtime(*cmd: str, expected_code: int = 0, **kwargs: Any) -> int:
    kwargs.setdefault("stdout", subprocess.PIPE)
    kwargs.setdefault("stderr", subprocess.PIPE)
    kwargs.setdefault("shell", True)
    proc = subprocess.Popen(cmd, **kwargs)
    while proc.poll() is None:
        if proc.stdout:
            line = proc.stdout.readline()
            if line == b"":
                break
            print(line.decode(), end="", flush=True)
    return_code = proc.poll()
    if expected_code is not None and return_code != expected_code:
        raise CalledProcessError(
            cmd,
            expected_code,
            return_code,
            proc.stdout,
            proc.stderr,
        )
    return return_code or expected_code


def paths_to_dbt_models(
    paths: Sequence[str],
    prefix: str = "",
    postfix: str = "",
) -> List[str]:
    return [prefix + get_filename(path) + postfix for path in paths]


def get_manifest(manifest_filename: str) -> Dict[str, Any]:
    try:
        manifest = Path(manifest_filename).read_text(encoding="utf-8")
        return json.loads(manifest)
    except Exception as e:
        raise ManifestOpenError(e)


def get_models(
    manifest: Dict[str, Any],
    filenames: Sequence[str],
) -> Generator[Dict[str, Any], None, None]:
    nodes = manifest.get("nodes", {})
    for key, node in nodes.items():
        if key.split(".")[-1] in filenames:
            yield node


def get_filename(path: str) -> str:
    return Path(path).stem


def get_filenames(paths: Sequence[str]) -> List[str]:
    return [get_filename(path) for path in paths]


def run_dbt_cmd(cmd: Sequence[Any]) -> int:
    try:
        return_code = cmd_realtime(*list(filter(None, cmd)), expected_code=0)
    except CalledProcessError:
        return_code = 1
    return return_code


def add_filenames_args(parser: ArgumentParser) -> NoReturn:
    parser.add_argument(
        "filenames",
        nargs="*",
        help="Filenames pre-commit believes are changed.",
    )


def add_dbt_cmd_args(parser: ArgumentParser) -> NoReturn:
    parser.add_argument(
        "--global-flags",
        nargs="*",
        help="Global dbt flags applicable to all subcommands.",
    )
    parser.add_argument(
        "--cmd-flags",
        nargs="*",
        help="Command-specific dbt flags.",
    )
    parser.add_argument(
        "--model-prefix",
        type=str,
        default="",
        help="Prefix dbt selector, for selecting parents.",
    )
    parser.add_argument(
        "--model-postfix",
        type=str,
        default="",
        help="Postfix dbt selector, for selecting children.",
    )
