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


def check_semantic_entities_have_required_fields(
    models: Sequence[SemanticModel],
    required_fields: Sequence[str],
) -> Tuple[int, Dict[str, List[str]]]:
    """
    Validate that each entity in each semantic model has the required top-level fields.

    Args:
        models: Iterable of SemanticModel instances.
        required_fields: List of field names that each entity must include.

    Returns:
        A tuple (status_code, issues) where:
          - status_code is 0 if all entities pass, 1 if any failures.
          - issues maps model names to lists of human-readable error messages.
    """
    status_code = 0
    issues: Dict[str, List[str]] = {}

    for model in models:
        model_issues: List[str] = []
        for entity in model.entities:
            name = entity.get("name", "<unnamed>")
            missing = [f for f in required_fields if not entity.get(f)]
            if missing:
                model_issues.append(
                    f"entity '{name}' missing fields: {', '.join(missing)}"
                )

        if model_issues:
            status_code = 1
            issues[model.name] = model_issues

    # Print any issues
    for model_name, problems in issues.items():
        print(f"{red(model_name)}: Entity field check failed")
        for msg in problems:
            print(f"  - {yellow(msg)}")

    return status_code, issues


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entrypoint: parse args, load manifests, extract SemanticModel objects,
    and validate that each entity has the required fields.
    """
    parser = argparse.ArgumentParser(
        description="Check semantic model entities have required fields"
    )
    add_default_args(parser)
    parser.add_argument(
        "--required-fields",
        nargs="+",
        default=["name", "type", "expr"],
        help="List of required fields for entities in semantic models",
    )
    args = parser.parse_args(argv)

    # Load semantic manifest (handles custom path, config override, default)
    try:
        semantic_manifest = get_dbt_semantic_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load semantic manifest file: {e}")
        return 1

    # Load dbt manifest (for tracking context)
    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load dbt manifest file: {e}")
        return 1

    # Extract typed models
    models = list(get_semantic_models(semantic_manifest))

    # Run validation
    start = time.time()
    status_code, _ = check_semantic_entities_have_required_fields(
        models=models,
        required_fields=args.required_fields,
    )
    elapsed = time.time() - start

    # Track hook execution
    tracker = dbtCheckpointTracking(script_args=vars(args))
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check semantic model entities have required fields",
            "status": status_code,
            "execution_time": elapsed,
            "is_pytest": vars(args).get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
