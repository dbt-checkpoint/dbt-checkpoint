import argparse
import operator
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_parent_childs,
    get_source_schemas,
)


def check_child_parent_cnt(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    required_cnt: Sequence[Dict[str, Any]],
    include_disabled: bool = False,
) -> Dict[str, Any]:
    status_code = 0
    ymls = [Path(path) for path in paths]

    schemas = get_source_schemas(ymls, include_disabled=include_disabled)

    for schema in schemas:
        childs = list(
            get_parent_childs(
                manifest=manifest,
                obj=schema,
                manifest_node="child_map",
                node_types=["model"],
            )
        )
        real_value = len(childs)
        for required in required_cnt:
            req_cnt = required.get("cnt")
            req_operator = required.get("operator", operator.lt)
            req_type = required.get("type")  # pragma: no mutate
            if req_cnt and req_operator(real_value, req_cnt):
                status_code = 1
                print(
                    f"{schema.source_name}.{schema.table_name}: "
                    f"has {real_value} {req_type}, but {req_type} {req_cnt}"
                    f"is/are required.",
                )

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--min-child-cnt",
        type=int,
        default=0,
        help="Minimal number of child relations.",
    )

    parser.add_argument(
        "--max-child-cnt",
        type=int,
        help="Miximal number of child relations.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    required_cnt = [
        {
            "operator": operator.lt,
            "type": "min",
            "dep": "childs",
            "cnt": args.min_child_cnt,
        },
        {
            "operator": operator.gt,
            "type": "max",
            "dep": "childs",
            "cnt": args.max_child_cnt,
        },
    ]

    start_time = time.time()
    hook_properties = check_child_parent_cnt(
        paths=args.filenames,
        manifest=manifest,
        required_cnt=required_cnt,
        include_disabled=args.include_disabled,
    )

    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the source has a specific number (max/min) "
            "of childs.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
