import argparse
import os
import time
from typing import Any, Dict, Set, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_missing_file_paths,
    get_model_sqls,
    get_models,
    Model,
    ParseDictOfLists,
)


def has_constraints(constraints:Dict[str, Set[str]], model:Model, nodes) -> bool:
    model_constraints = nodes.get(model.model_id).get("constraints")
    for constraint_type, columns in constraints:
        if (constraint_type not in model_constraints
                or columns != set(model_constraints.get(constraint_type))):
            return False
    return True


def is_incremental_or_table(model:Model, nodes) -> bool:
    materialization = nodes.get(model.model_id).get("config").get("materialized")
    return (materialization == "table" or materialization == "incremental")


def check_constraints(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    constraints: Dict[str, Set[str]],
    exclude_pattern: str,
    include_disabled: bool = False
) -> int:
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
    )

    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())
    models = get_models(manifest, filenames, include_disabled=include_disabled)
    nodes = manifest.get("nodes", {})
    status_code = 0

    for model in (model for model in models if is_incremental_or_table(model, nodes)):
        if not has_constraints(constraints, model, nodes):
            status_code = 1
            print(
                f"{model.model_name}: "
                "doesn't have necessary constraints defined.",
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--constraints",
        metavar="KEY=VALUE1,VALUE2",
        nargs="+",
        required=True,
        help="Set a number of key-value pairs. I.e primary_key=column1,column2"
        " Key is a type of a constraint and values are column names"
        "(do not put spaces before or after the = sign or commas). "
        "",
        action=ParseDictOfLists,
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code = check_constraints(
        paths=args.filenames,
        manifest=manifest,
        constraints=args.constraints.items(),
        exclude_pattern=args.exclude,
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
            "description": "Check model has constraints",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
