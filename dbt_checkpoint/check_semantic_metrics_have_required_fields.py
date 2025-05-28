import argparse
import os
import time
from typing import Sequence, Optional, Tuple, Dict, List

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_dbt_semantic_manifest,
    red,
    yellow,
    SemanticLayerMetric,
    get_semantic_layer_metrics,
)


def check_semantic_metrics_have_required_fields(
    metrics: Sequence[SemanticLayerMetric],
    required_fields: Sequence[str],
) -> Tuple[int, Dict[str, List[str]]]:
    """
    Validate that each semantic layer metric includes the required top-level fields.

    Args:
        metrics: Sequence of SemanticLayerMetric instances.
        required_fields: List of metric attribute names that must be non-empty.

    Returns:
        status_code: 0 if all metrics pass validation, 1 otherwise.
        issues: Mapping from metric name to list of missing-field messages.
    """
    status_code = 0
    issues: Dict[str, List[str]] = {}

    for metric in metrics:
        metric_name = metric.name or "<unnamed>"
        # Check each required field on the dataclass
        missing_fields = [
            field
            for field in required_fields
            # getattr returns the attribute or None if missing
            if not getattr(metric, field, None)
        ]

        if missing_fields:
            status_code = 1
            issues[metric_name] = [
                f"missing field: {field}" for field in missing_fields
            ]

    # Print any failures
    for metric_name, errs in issues.items():
        print(f"{red(metric_name)}: Metric field check failed")
        for err in errs:
            print(f"  - {yellow(err)}")

    return status_code, issues


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entry point. Parses arguments, loads manifests, extracts metrics,
    and validates that each metric has the required fields.
    """
    parser = argparse.ArgumentParser(
        description="Check semantic model metrics have required fields"
    )
    add_default_args(parser)
    parser.add_argument(
        "--required-fields",
        nargs="+",
        default=["name", "type", "description", "label"],
        help="List of required fields for semantic layer metrics",
    )
    args = parser.parse_args(argv)

    # Load the semantic manifest JSON
    try:
        semantic_manifest = get_dbt_semantic_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load semantic manifest file: {e}")
        return 1

    # Load the dbt manifest for tracking metadata
    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load dbt manifest file: {e}")
        return 1

    # Extract typed metric objects
    metrics = list(get_semantic_layer_metrics(semantic_manifest))

    # Perform validation
    start_time = time.time()
    status_code, _ = check_semantic_metrics_have_required_fields(
        metrics=metrics,
        required_fields=args.required_fields,
    )
    elapsed = time.time() - start_time

    # Track the hook execution
    tracker = dbtCheckpointTracking(script_args=vars(args))
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check semantic model metrics have required fields",
            "status": status_code,
            "execution_time": elapsed,
            "is_pytest": vars(args).get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
