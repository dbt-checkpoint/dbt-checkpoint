import argparse
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

from dbt_checkpoint.utils import add_config_args
from dbt_checkpoint.utils import add_dbt_cmd_args
from dbt_checkpoint.utils import add_dbt_cmd_model_args
from dbt_checkpoint.utils import add_filenames_args
from dbt_checkpoint.utils import extend_dbt_project_dir_flag
from dbt_checkpoint.utils import get_config_file
from dbt_checkpoint.utils import get_flags
from dbt_checkpoint.utils import paths_to_dbt_models
from dbt_checkpoint.utils import run_dbt_cmd


def prepare_cmd(
    dbt_command: str,
    paths: Sequence[str],
    global_flags: Optional[Sequence[str]] = None,
    cmd_flags: Optional[Sequence[str]] = None,
    prefix: str = "",
    postfix: str = "",
    models: Optional[Sequence[str]] = None,
    config: Dict[str, Any] = {},
) -> List[str]:
    global_flags = get_flags(global_flags)
    cmd_flags = get_flags(cmd_flags)
    dbt_project_dir = config.get("dbt-project-dir")
    if models:
        dbt_models = models
    else:
        dbt_models = paths_to_dbt_models(paths, prefix, postfix)
    cmd = ["dbt", *global_flags, dbt_command, "-m", *dbt_models, *cmd_flags]
    return extend_dbt_project_dir_flag(cmd, cmd_flags, dbt_project_dir)  # type: ignore


def parse_and_run(dbt_command: str, argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_dbt_cmd_args(parser)
    add_dbt_cmd_model_args(parser)
    add_config_args(parser)

    args = parser.parse_args(argv)
    config = get_config_file(args.config)

    cmd = prepare_cmd(
        dbt_command=dbt_command,
        paths=args.filenames,
        global_flags=args.global_flags,
        cmd_flags=args.cmd_flags,
        prefix=args.model_prefix,
        postfix=args.model_postfix,
        models=args.models,
        config=config,
    )
    return run_dbt_cmd(cmd)
