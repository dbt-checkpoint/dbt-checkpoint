import argparse
import os
from typing import List, Optional, Sequence

from dbt_checkpoint.utils import (
    add_dbt_project_dir_args,
    add_default_args,
    get_flags,
    paths_to_dbt_models,
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


def propagate_project_dir(dbt_project_dir: str) -> int:
    os.environ["DBT_PROJECT_DIR"] = dbt_project_dir
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    add_dbt_project_dir_args(parser)
    args = parser.parse_args(argv)
    dbt_project_dir = args.project_dir
    return propagate_project_dir(dbt_project_dir)


if __name__ == "__main__":
    exit(main())
