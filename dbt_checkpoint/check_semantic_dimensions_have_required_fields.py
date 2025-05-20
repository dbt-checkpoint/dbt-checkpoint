import argparse
import json
import os
import time
from typing import Any, Dict, Optional, Sequence, Tuple, List

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    red,
    yellow,
    get_dbt_semantic_manifest,
    get_dbt_manifest,
)


def get_nested_value(obj: Dict[str, Any], path: str) -> Any:
    keys = path.split(".")
    for key in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


def check_semantic_dimensions_have_fields(
    semantic_manifest: Dict[str, Any], required_fields: Sequence[str]
) -> Tuple[int, Dict[str, Any]]:
    """
    Checks that each dimension in a semantic model has required fields.
    Ensures that:
      - required_fields like name/type/expr/config.meta.displayName are present
      - time dimensions have type_params.time_granularity

    Returns:
        status_code: 0 if all checks pass, 1 otherwise.
        issues: Dictionary of problems per semantic model.
    """
    status_code = 0
    issues: Dict[str, List[str]] = {}

    for model in semantic_manifest.get("semantic_models", []):
        model_issues = []
        model_name = model.get("name", "<unnamed>")

        for dim in model.get("dimensions", []):
            dim_name = dim.get("name", "<unnamed>")
            missing = []

            for field in required_fields:
                value = get_nested_value(dim, field) if "." in field else dim.get(field)
                if not value:
                    missing.append(field)

            if missing:
                model_issues.append(
                    f"dimension '{dim_name}' missing fields: {', '.join(missing)}"
                )

            if dim.get("type") == "time":
                if not dim.get("type_params", {}).get("time_granularity"):
                    model_issues.append(
                        f"dimension '{dim_name}' is type 'time' but missing type_params.time_granularity"
                    )

        if model_issues:
            status_code = 1
            issues[model_name] = model_issues

    for model, problems in issues.items():
        print(f"{red(model)}: Dimension field check failed")
        for prob in problems:
            print(f"  - {yellow(prob)}")

    return status_code, issues


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    parser.add_argument(
        "--required-fields",
        nargs="+",
        default=["name", "type", "expr", "config.meta.displayName"],
        help="List of required fields for dimensions in semantic models",
    )
    args = parser.parse_args(argv)

    try:
        semantic_manifest = get_dbt_semantic_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code, _ = check_semantic_dimensions_have_fields(
        semantic_manifest=semantic_manifest,
        required_fields=args.required_fields,
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check semantic model dimensions have required fields",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
