import argparse
import os
import time
from typing import Any, Dict, List, Optional, Sequence

from dbt_checkpoint.utils import (
    add_dbt_cmd_args,
    add_dbt_cmd_model_args,
    add_filenames_args,
    get_flags,
    paths_to_dbt_models,
    run_dbt_cmd,
)


def prepare_cmd(
    paths: Sequence[str],
    global_flags: Optional[Sequence[str]] = None,
    cmd_flags: Optional[Sequence[str]] = None,
    prefix: str = "",
    postfix: str = "",
    models: Optional[Sequence[str]] = None,
) -> List[str]:
    global_flags = get_flags(global_flags)
    cmd_flags = get_flags(cmd_flags)
    if models:
        dbt_models = models
    else:
        dbt_models = paths_to_dbt_models(paths, prefix, postfix)
    cmd = ["dbt", *global_flags, "test", "-m", *dbt_models, *cmd_flags]
    return cmd


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_dbt_cmd_args(parser)
    add_dbt_cmd_model_args(parser)

    args = parser.parse_args(argv)

    cmd = prepare_cmd(
        args.filenames,
        args.global_flags,
        args.cmd_flags,
        args.model_prefix,
        args.model_postfix,
        args.models,
    )
    return run_dbt_cmd(cmd)


if __name__ == "__main__":
    exit(main())
