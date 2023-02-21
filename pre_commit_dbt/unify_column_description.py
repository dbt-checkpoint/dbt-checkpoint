import argparse
import os
import time
from collections import Counter
from pathlib import Path
from typing import Any
from typing import Dict
from typing import NoReturn
from typing import Optional
from typing import Sequence

import yaml

from pre_commit_dbt.check_column_desc_are_same import get_grouped
from pre_commit_dbt.tracking import dbtCheckpointTracking
from pre_commit_dbt.utils import add_default_args
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import JsonOpenError


def _replace_desc(path: Path, column_name: str, description: str) -> NoReturn:
    file = yaml.safe_load(path.open())
    for model in file.get("models", []):
        for column in model.get("columns", []):
            if column_name == column.get("name"):
                column["description"] = description
    with open(path, "w") as f:
        yaml.dump(file, f, default_flow_style=False, sort_keys=False)
        print(
            f"{path}: replaced description of "
            f"column `{column_name}` for `{description}`"
        )


def replace_column_desc(
    paths: Sequence[str], ignore: Optional[Sequence[str]]
) -> Dict[str, Any]:
    status_code = 0
    grouped = get_grouped(paths, ignore)

    for name, grps in grouped:
        groups = list(grps)
        group_cnt = Counter([group.description for group in groups])
        if len(group_cnt.keys()) > 1:
            status_code = 1
            group_with_desc = Counter(
                [group.description for group in groups if group.description]
            )
            top_two = dict(group_with_desc.most_common(2))  # pragma: no mutate
            top_desc = group_with_desc.most_common(1)[0][0]  # pragma: no mutate
            if (
                len(set(top_two.values())) == 1
                and len(group_with_desc.keys()) > 1  # pragma: no mutate
            ):
                desc = [f"{key} (count: {value})" for key, value in top_two.items()]
                result = "\n- ".join(list(desc))  # pragma: no mutate
                print(
                    f"I can't determine which label should be the default:\n- {result}"
                )
            else:
                for group in groups:
                    if group.description != top_desc:
                        _replace_desc(group.file, name, top_desc)

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--ignore",
        nargs="*",
        help="Columns for which do not check whether have a different description.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = replace_column_desc(paths=args.filenames, ignore=args.ignore)
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Unify column descriptions across all models.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
