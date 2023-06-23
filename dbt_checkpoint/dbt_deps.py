import argparse
import os
import time
from typing import Any, Dict, List, Optional, Sequence

from dbt_checkpoint.utils import (
    add_dbt_cmd_args,
    get_config_file,
    get_flags,
    run_dbt_cmd,
)


def prepare_cmd(
    global_flags: Optional[Sequence[str]] = None,
    cmd_flags: Optional[Sequence[str]] = None,
    config: Dict[str, Any] = {},
) -> List[str]:
    global_flags = get_flags(global_flags)
    cmd_flags = get_flags(cmd_flags)
    dbt_project_dir = config.get("dbt-project-dir")
    cmd = ["dbt", *global_flags, "deps", *cmd_flags]
    if dbt_project_dir and not "--project-dir" in cmd_flags:
        cmd.extend(["--project-dir", dbt_project_dir])
    return cmd


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_dbt_cmd_args(parser)

    args = parser.parse_args(argv)
    config = get_config_file(args.config)

    cmd = prepare_cmd(args.global_flags, args.cmd_flags, config)
    return run_dbt_cmd(cmd)


if __name__ == "__main__":
    exit(main())
