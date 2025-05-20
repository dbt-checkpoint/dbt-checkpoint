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


def check_semantic_entities_have_fields(
    semantic_manifest: Dict[str, Any], required_fields: Sequence[str]
) -> Tuple[int, Dict[str, Any]]:
    """
    Checks that each entity under a semantic model has the required fields.

    Returns:
        status_code: 0 if all checks pass, 1 otherwise.
        issues: Dictionary of problems per semantic model.
    """
    status_code = 0
    issues: Dict[str, List[str]] = {}

    for model in semantic_manifest.get("semantic_models", []):
        model_issues = []
        model_name = model.get("name", "<unnamed>")

        for entity in model.get("entities", []):
            entity_name = entity.get("name", "<unnamed>")
            missing = [f for f in required_fields if not entity.get(f)]

            if missing:
                model_issues.append(
                    f"entity '{entity_name}' missing fields: {', '.join(missing)}"
                )

        if model_issues:
            status_code = 1
            issues[model_name] = model_issues

    for model, problems in issues.items():
        print(f"{red(model)}: Entity field check failed")
        for prob in problems:
            print(f"  - {yellow(prob)}")

    return status_code, issues


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    parser.add_argument(
        "--required-fields",
        nargs="+",
        default=["name", "type", "expr"],
        help="List of required fields for entities in semantic models",
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
    status_code, _ = check_semantic_entities_have_fields(
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
            "description": "Check semantic model entities have required fields",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
