import argparse
import os
import time
from typing import Any, Dict, List, Optional, Sequence

from dbt_checkpoint.utils import (
    add_config_args,
    add_dbt_cmd_args,
    extend_dbt_cloud_flags,
    extend_dbt_project_dir_flag,
    get_config_file,
    get_flags,
    run_dbt_cmd,
)


def prepare_cmd(
    global_flags: Optional[Sequence[str]] = None,
    cmd_flags: Optional[Sequence[str]] = None,
    config: Dict[str,Any] = {}
) -> List[str]:
    global_flags = get_flags(global_flags)
    cmd_flags = get_flags(cmd_flags)

    dbt_project_dir = config.get("dbt-project-dir")
    cmd = ["dbt", *global_flags, "clean", *cmd_flags]
    cmd = extend_dbt_project_dir_flag(cmd, cmd_flags, dbt_project_dir)
    cmd = extend_dbt_cloud_flags(cmd, config)
    return cmd


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_dbt_cmd_args(parser)
    add_config_args(parser)

    args = parser.parse_args(argv)
    dbt_checkpoint_config = get_config_file(args.config)

    cmd = prepare_cmd(args.global_flags, args.cmd_flags, dbt_checkpoint_config)
    return run_dbt_cmd(cmd)


if __name__ == "__main__":
    exit(main())
