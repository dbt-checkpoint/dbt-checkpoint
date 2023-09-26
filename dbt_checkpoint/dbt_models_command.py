import argparse
import os
import time
from typing import Any, Dict, List, Optional, Sequence

from dbt_checkpoint.utils import (
    add_config_args,
    add_dbt_cmd_args,
    add_dbt_cmd_model_args,
    add_filenames_args,
    extend_dbt_project_dir_flag,
    get_config_file,
    get_flags,
    paths_to_dbt_models,
    run_dbt_cmd,
)


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
    return extend_dbt_project_dir_flag(cmd, cmd_flags, dbt_project_dir)


def parse_and_run(dbt_command:str, argv: Optional[Sequence[str]] = None) -> int:
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
        config=config
    )
    return run_dbt_cmd(cmd)
