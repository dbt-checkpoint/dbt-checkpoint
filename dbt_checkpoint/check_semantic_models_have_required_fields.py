import argparse
import json
import os
import time
from typing import Any, Dict, Optional, Sequence, Set, Tuple, List

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    add_default_args,
    red,
    yellow,
)


def check_semantic_manifest(
    paths: Sequence[str],
    required_entity_fields: Sequence[str],
    required_entity_meta_keys: Sequence[str],
) -> Tuple[int, Dict[str, Any]]:
    """
    Checks that each semantic model has required fields and entities meet structural/meta criteria.

    Returns:
        status_code: 0 if all checks pass, 1 otherwise.
        issues: Dictionary of problems per semantic model.
    """
    status_code = 0
    issues: Dict[str, List[str]] = {}

    print(path)
    for path in paths:
        try:
            with open(path) as f:
                semantic_manifest = json.load(f)
        except Exception as e:
            print(f"Unable to load file {path}: {e}")
            return 1, {}

        for model in semantic_manifest.get("semantic_models", []):
            model_issues = []
            name = model.get("name", "<unnamed>")

            if not model.get("description"):
                model_issues.append("missing description")

            if not model.get("node_relation"):
                model_issues.append("missing node_relation")

            for entity in model.get("entities", []):
                entity_name = entity.get("name", "<unnamed>")
                missing_fields = [
                    field for field in required_entity_fields if not entity.get(field)
                ]
                if missing_fields:
                    model_issues.append(
                        f"entity '{entity_name}' missing fields: {', '.join(missing_fields)}"
                    )

                meta = entity.get("config", {}).get("meta", {})
                missing_meta_keys = [
                    k for k in required_entity_meta_keys if k not in meta
                ]
                if missing_meta_keys:
                    model_issues.append(
                        f"entity '{entity_name}' missing meta keys: {', '.join(missing_meta_keys)}"
                    )

            if model_issues:
                status_code = 1
                issues[name] = model_issues

    for model, problems in issues.items():
        print(f"{red(model)}: Semantic model failed validation")
        for prob in problems:
            print(f"  - {yellow(prob)}")

    return status_code, issues


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    parser.add_argument("--entity-fields", nargs="+", default=["name", "type", "expr"])
    parser.add_argument("--entity-meta-keys", nargs="+", default=[])
    args = parser.parse_args(argv)

    start_time = time.time()
    status_code, _ = check_semantic_manifest(
        paths=args.filenames,
        required_entity_fields=args.entity_fields,
        required_entity_meta_keys=args.entity_meta_keys,
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=None,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check semantic layer models/entities for required structure",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
