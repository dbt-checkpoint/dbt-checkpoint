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
    SemanticModel,
    get_semantic_models,
)


def check_semantic_measures_have_required_fields(
    models: Sequence[SemanticModel],
    required_fields: Sequence[str],
) -> Tuple[int, Dict[str, List[str]]]:
    """
    Validate that each measure in each SemanticModel has the required fields.

    Args:
        models: Sequence of SemanticModel instances.
        required_fields: List of top-level measure fields that must be present.

    Returns:
        status_code: 0 if all measures pass validation, 1 otherwise.
        issues: A mapping from model name to lists of error messages.
    """
    status_code = 0
    issues: Dict[str, List[str]] = {}

    # Iterate through each model and its measures
    for model in models:
        model_issues: List[str] = []
        for measure in model.measures:
            measure_name = measure.get("name", "<unnamed>")
            missing = [f for f in required_fields if not measure.get(f)]
            if missing:
                model_issues.append(
                    f"measure '{measure_name}' missing fields: {', '.join(missing)}"
                )

        if model_issues:
            status_code = 1
            issues[model.name] = model_issues

    # Print any issues found
    for model_name, problems in issues.items():
        print(f"{red(model_name)}: Measure field check failed")
        for msg in problems:
            print(f"  - {yellow(msg)}")

    return status_code, issues


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entrypoint: parse arguments, load manifests, extract SemanticModel objects,
    and validate measure fields.
    """
    parser = argparse.ArgumentParser(
        description="Check semantic model measures have required fields"
    )
    add_default_args(parser)
    parser.add_argument(
        "--required-fields",
        nargs="+",
        default=["name", "expr", "agg"],
        help="List of required fields for measures in semantic models",
    )
    args = parser.parse_args(argv)

    # Load semantic manifest JSON
    try:
        semantic_manifest = get_dbt_semantic_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load semantic manifest file: {e}")
        return 1

    # Load dbt manifest for tracking
    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load dbt manifest file: {e}")
        return 1

    # Extract typed SemanticModel objects
    models = list(get_semantic_models(semantic_manifest))

    # Perform validation
    start_time = time.time()
    status_code, _ = check_semantic_measures_have_required_fields(
        models=models,
        required_fields=args.required_fields,
    )
    elapsed = time.time() - start_time

    # Track hook execution event
    tracker = dbtCheckpointTracking(script_args=vars(args))
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check semantic model measures have required fields",
            "status": status_code,
            "execution_time": elapsed,
            "is_pytest": vars(args).get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
