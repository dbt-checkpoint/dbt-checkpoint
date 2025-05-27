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

FIELD_MAPPING: Dict[str, str] = {
    "model": "node_relation",
}


def check_semantic_models_have_required_fields(
    models: Sequence[SemanticModel],
    required_fields: Sequence[str],
) -> Tuple[int, Dict[str, List[str]]]:
    """
    Validate each SemanticModel has the required top-level fields.
    Also ensures that if a model defines measures, it specifies `defaults.agg_time_dimension`.

    Args:
        models: Sequence of SemanticModel instances.
        required_fields: List of field names (top‑level or aliases) that must be present.

    Returns:
        A tuple (status_code, issues) where:
          - status_code is 0 if all models pass, 1 if any failures.
          - issues maps model names to lists of error messages.
    """
    status_code = 0
    issues: Dict[str, List[str]] = {}
    # Resolve any aliases (e.g., "model" → "node_relation")
    resolved_fields = [FIELD_MAPPING.get(f, f) for f in required_fields]

    for model in models:
        model_issues: List[str] = []
        for field in resolved_fields:
            # Use getattr since dataclass attributes map to actual properties
            if not getattr(model, field, None):
                model_issues.append(f"missing {field}")

        # If the model has measures, ensure agg_time_dimension is set
        if model.measures:
            agg_time = model.defaults.get("agg_time_dimension")
            if not agg_time:
                model_issues.append(
                    "missing defaults.agg_time_dimension despite having measures"
                )

        if model_issues:
            status_code = 1
            issues[model.name] = model_issues

    # Print any validation failures
    for model_name, problems in issues.items():
        print(f"{red(model_name)}: Semantic model failed validation")
        for prob in problems:
            print(f"  - {yellow(prob)}")

    return status_code, issues


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entrypoint: parse args, load manifests, extract SemanticModel objects,
    and validate that each semantic model has user‑specified required fields.
    """
    parser = argparse.ArgumentParser(
        description="Check semantic models have required fields"
    )
    add_default_args(parser)
    parser.add_argument(
        "--required-fields",
        nargs="+",
        default=["description", "model"],
        help="List of required fields for semantic models (e.g., description model)",
    )
    args = parser.parse_args(argv)

    # Load semantic manifest
    try:
        semantic_manifest = get_dbt_semantic_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load semantic manifest file: {e}")
        return 1

    # Load dbt manifest for tracking context
    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file: {e}")
        return 1

    # Extract strongly‑typed models
    models = list(get_semantic_models(semantic_manifest))

    # Run validation
    start_time = time.time()
    status_code, _ = check_semantic_models_have_required_fields(
        models=models,
        required_fields=args.required_fields,
    )
    elapsed = time.time() - start_time

    # Track hook execution
    tracker = dbtCheckpointTracking(script_args=vars(args))
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check semantic model has required fields",
            "status": status_code,
            "execution_time": elapsed,
            "is_pytest": vars(args).get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
