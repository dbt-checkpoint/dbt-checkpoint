import argparse
import os
import time
from typing import Sequence, Optional, Tuple, Dict, List

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_semantic_manifest,
    get_dbt_manifest,
    red,
    yellow,
    SemanticModel,
    get_semantic_models,
)


def check_semantic_dimensions_have_required_fields(
    models: Sequence[SemanticModel],
    required_fields: Sequence[str],
) -> Tuple[int, Dict[str, List[str]]]:
    """
    Ensure each dimension in each SemanticModel has the required fields.
    Also enforces that dimensions of type 'time' include 'time_granularity' under 'type_params'.

    Args:
        models: Sequence of SemanticModel instances.
        required_fields: Sequence of dimension keys that must be present.

    Returns:
        status_code: 0 if all dimensions pass validation, 1 otherwise.
        issues: Mapping from model name to list of error messages.
    """
    status_code = 0
    issues: Dict[str, List[str]] = {}

    # Iterate over all semantic models
    for model in models:
        model_issues: List[str] = []

        # Check each dimension within the model
        for dim in model.dimensions:
            dim_name = dim.get("name", "<unnamed>")
            missing: List[str] = []

            # Check top-level required fields
            for field in required_fields:
                if not dim.get(field):
                    missing.append(field)

            # Enforce time granularity for time dimensions
            if dim.get("type") == "time":
                time_gran = dim.get("type_params", {}).get("time_granularity")
                if not time_gran:
                    missing.append("type_params.time_granularity")

            # Record any missing fields
            if missing:
                model_issues.append(
                    f"dimension '{dim_name}' missing fields: {', '.join(missing)}"
                )

        if model_issues:
            status_code = 1
            issues[model.name or "<unnamed>"] = model_issues

    # Print formatted output for any issues
    for model_name, problems in issues.items():
        print(f"{red(model_name)}: Dimension field check failed")
        for prob in problems:
            print(f"  - {yellow(prob)}")

    return status_code, issues


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entry point. Parses arguments, loads manifests, extracts SemanticModel
    objects, and runs dimension field validation.
    """
    parser = argparse.ArgumentParser(
        description="Validate semantic dimension required fields"
    )
    add_default_args(parser)
    parser.add_argument(
        "--required-fields",
        nargs="+",
        default=["name", "type"],
        help="List of required top-level fields for dimensions in semantic models",
    )
    args = parser.parse_args(argv)

    # Load semantic manifest (with custom path precedence)
    try:
        semantic_manifest = get_dbt_semantic_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load semantic manifest file: {e}")
        return 1

    # Load dbt manifest for tracking metadata
    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load dbt manifest file: {e}")
        return 1

    # Extract typed models
    models = list(get_semantic_models(semantic_manifest))

    # Perform validation
    start_time = time.time()
    status_code, _ = check_semantic_dimensions_have_required_fields(
        models=models,
        required_fields=args.required_fields,
    )
    end_time = time.time()

    # Track execution event
    tracker = dbtCheckpointTracking(script_args=vars(args))
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check semantic model dimensions have required fields",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": vars(args).get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
