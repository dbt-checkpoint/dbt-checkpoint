from typing import List
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import run_dbt_cmd


def prepare_cmd() -> List[str]:
    cmd = ["dbt", "deps"]
    return cmd


def main(argv: Optional[Sequence[str]] = None) -> int:
    cmd = prepare_cmd()
    return run_dbt_cmd(cmd)


if __name__ == "__main__":
    exit(main())
