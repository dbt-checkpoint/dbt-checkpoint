import argparse
from typing import List
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_dbt_cmd_prj_root
from pre_commit_dbt.utils import run_dbt_cmd


def prepare_cmd() -> List[str]:
    cmd = ["dbt", "deps"]
    return cmd


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()    
    add_dbt_cmd_prj_root(parser)

    args = parser.parse_args(argv)

    cmd = prepare_cmd()
    return run_dbt_cmd(cmd, prj_root=args.prj_root)


if __name__ == "__main__":
    exit(main())
