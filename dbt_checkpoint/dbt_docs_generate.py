import argparse
import os
import time
from typing import Any, Dict, List, Optional, Sequence

from dbt_checkpoint.utils import add_dbt_cmd_args, get_flags, run_dbt_cmd


def docs_generate_cmd(
    global_flags: Optional[Sequence[str]] = None,
    cmd_flags: Optional[Sequence[str]] = None,
) -> List[str]:
    global_flags = get_flags(global_flags)
    cmd_flags = get_flags(cmd_flags)
    cmd = ["dbt", *global_flags, "docs", "generate", *cmd_flags]
    return cmd


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_dbt_cmd_args(parser)
    print(f"{k}:{v}" for k, v in os.environ.items())

    args = parser.parse_args(argv)

    cmd = docs_generate_cmd(args.global_flags, args.cmd_flags)
    return run_dbt_cmd(cmd)


if __name__ == "__main__":
    exit(main())
