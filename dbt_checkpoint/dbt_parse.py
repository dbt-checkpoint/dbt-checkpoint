import argparse
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

from dbt_checkpoint.utils import add_config_args
from dbt_checkpoint.utils import add_dbt_cmd_args
from dbt_checkpoint.utils import extend_dbt_project_dir_flag
from dbt_checkpoint.utils import get_config_file
from dbt_checkpoint.utils import get_flags
from dbt_checkpoint.utils import run_dbt_cmd


def prepare_cmd(
    global_flags: Optional[Sequence[str]] = None,
    cmd_flags: Optional[Sequence[str]] = None,
    config: Dict[str, Any] = {},
) -> List[str]:
    global_flags = get_flags(global_flags)
    cmd_flags = get_flags(cmd_flags)
    dbt_project_dir = config.get("dbt-project-dir")
    cmd = ["dbt", *global_flags, "parse", *cmd_flags]
    return extend_dbt_project_dir_flag(cmd, cmd_flags, dbt_project_dir)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_dbt_cmd_args(parser)
    add_config_args(parser)

    args = parser.parse_args(argv)
    config = get_config_file(args.config)

    cmd = prepare_cmd(args.global_flags, args.cmd_flags, config)
    return run_dbt_cmd(cmd)


if __name__ == "__main__":
    exit(main())
