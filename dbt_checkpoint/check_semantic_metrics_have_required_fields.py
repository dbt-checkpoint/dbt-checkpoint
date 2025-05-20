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


def check_semantic_metrics_have_fields(
    semantic_manifest: Dict[str, Any], required_fields: Sequence[str]
) -> Tuple[int, Dict[str, Any]]:
    """
    Checks that each metric has the required fields.
    If a field starts with config.meta., it looks up the corresponding measure.

    Returns:
        status_code: 0 if all checks pass, 1 otherwise.
        issues: Dictionary of problems per metric.
    """
    status_code = 0
    issues: Dict[str, List[str]] = {}

    # Index all measures by name across all semantic models
    measure_map = {
        measure["name"]: measure
        for model in semantic_manifest.get("semantic_models", [])
        for measure in model.get("measures", [])
    }

    for metric in semantic_manifest.get("metrics", []):
        metric_name = metric.get("name", "<unnamed>")
        metric_issues = []

        for field in required_fields:
            if field.startswith("config.meta."):
                meta_field = field.replace("config.meta.", "")
                measure_name = get_nested_value(metric, "type_params.measure.name")
                measure = measure_map.get(measure_name)
                if not measure or not measure.get("config", {}).get("meta", {}).get(
                    meta_field
                ):
                    metric_issues.append(field)
            else:
                value = (
                    get_nested_value(metric, field)
                    if "." in field
                    else metric.get(field)
                )
                if value in [None, ""]:
                    metric_issues.append(field)

        if metric_issues:
            status_code = 1
            issues[metric_name] = metric_issues

    for metric, fields in issues.items():
        print(f"{red(metric)}: Metric field check failed")
        print(f"  - Missing: {yellow(', '.join(fields))}")

    return status_code, issues


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    parser.add_argument(
        "--required-fields",
        nargs="+",
        default=[
            "name",
            "type",
            "description",
            "label",
            "type_params.measure.name",
            "config.meta.isDefaultMetric",
            "config.meta.displayFormat",
            "config.meta.displayName",
        ],
        help="List of required fields for metrics (config.meta.* checked in corresponding measure)",
    )
    args = parser.parse_args(argv)

    try:
        semantic_manifest = get_dbt_semantic_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load semantic manifest file ({e})")
        return 1

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code, _ = check_semantic_metrics_have_fields(
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
            "description": "Check semantic model metrics have required fields",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
