import argparse
import os
import time
from typing import Any, Dict, Optional, Sequence, Set

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_missing_file_paths,
    get_model_sqls,
    get_models,
    Model,
)

def is_incremental_or_table(model:Model) -> bool:
    materialized = model.node.get("config").get("materialized")
    return (materialized == "table" or materialized == "incremental")

def extract_constraint_types(model_constraints:Sequence[str]) -> Set[str]:
    return {constraint.get("type") for constraint in model_constraints}

def missing_generic_constraints(constraints:Sequence[Dict[str, Any]], model:Model) -> Set[str]:
    model_constraints = model.node.get("constraints", [])
    generic_constraints = extract_constraint_types(model_constraints)
    return set(constraints) - generic_constraints

def check_generic_constraints(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    constraints: Sequence[str],
    exclude_pattern: str,
    include_disabled: bool = False
) -> int:
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
    )

    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())
    models = get_models(manifest, filenames, include_disabled=include_disabled)
    status_code = 0

    for model in (model for model in models if is_incremental_or_table(model)):
        missing_constraints = missing_generic_constraints(constraints, model)
        if missing_constraints:
            status_code = 1
            print(
                f"{model.model_id}: "
                "Doesn't have necessary generic constraints defined, it's missing:",
                f"{missing_constraints}"
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:

    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--constraints",
        metavar="<constraint>",
        required=True,
        nargs="+",
        type=str,
        help="Set a list of constraint types to validate (e.g.: primary_key unique)",
    )
    print(argv)
    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code = check_generic_constraints(
        paths=args.filenames,
        manifest=manifest,
        constraints=args.constraints,
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
            "description": "Check model has generic constraints",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
