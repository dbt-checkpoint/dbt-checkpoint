import argparse
from typing import List
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_dbt_cmd_args
from pre_commit_dbt.utils import get_flags
from pre_commit_dbt.utils import run_dbt_cmd


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

    args = parser.parse_args(argv)

    cmd = docs_generate_cmd(args.global_flags, args.cmd_flags)
    return run_dbt_cmd(cmd)


if __name__ == "__main__":
    exit(main())
