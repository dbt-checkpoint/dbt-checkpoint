import argparse
import operator
import os
import time
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_model_sqls,
    get_models,
    get_parent_childs,
)


def check_child_parent_cnt(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    required_cnt: Sequence[Dict[str, Any]],
    include_disabled: bool = False,
) -> int:
    status_code = 0
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames, include_disabled=include_disabled)

    for model in models:
        childs = list(
            get_parent_childs(
                manifest=manifest,
                obj=model,
                manifest_node="child_map",
                node_types=["model"],
            )
        )
        parents = list(
            get_parent_childs(
                manifest=manifest,
                obj=model,
                manifest_node="parent_map",
                node_types=["model", "source"],
            )
        )
        real_cnt = {"childs": len(childs), "parents": len(parents)}
        for required in required_cnt:
            req_cnt = required.get("cnt")
            req_operator = required.get("operator", operator.lt)
            req_type = required.get("type")  # pragma: no mutate
            req_dep = required.get("dep", "")  # pragma: no mutate
            real_value = real_cnt.get(req_dep)
            if req_cnt and req_operator(real_value, req_cnt):
                status_code = 1
                print(
                    f"{model.model_name}: "
                    f"has {real_value} {req_dep} {req_type}, but {req_type} {req_cnt} "
                    f"is/are required.",
                )

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--min-parent-cnt",
        type=int,
        default=0,
        help="Minimal number of parent relations.",
    )

    parser.add_argument(
        "--max-parent-cnt",
        type=int,
        help="Maximal number of parent relations.",
    )

    parser.add_argument(
        "--min-child-cnt",
        type=int,
        default=0,
        help="Minimal number of child relations.",
    )

    parser.add_argument(
        "--max-child-cnt",
        type=int,
        help="Maximal number of child relations.",
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
            "dep": "parents",
            "cnt": args.min_parent_cnt,
        },
        {
            "operator": operator.gt,
            "type": "max",
            "dep": "parents",
            "cnt": args.max_parent_cnt,
        },
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
    status_code = check_child_parent_cnt(
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
            "description": "Check model has parents and childs",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
