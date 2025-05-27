import argparse
import os
import time
from typing import Sequence, Optional

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    add_meta_keys_args,
    get_dbt_manifest,
    get_dbt_semantic_manifest,
    red,
    yellow,
    SemanticModel,
    get_semantic_models,
)


def validate_meta_keys(
    actual: Sequence[str], expected: Sequence[str], allow_extra_keys: bool
) -> bool:
    """
    Compare actual and expected meta keys.
    If allow_extra_keys is True, actual may contain more than expected.
    Otherwise, actual must exactly match expected.
    """
    actual_set = set(actual)
    expected_set = set(expected)
    return (
        expected_set.issubset(actual_set)
        if allow_extra_keys
        else actual_set == expected_set
    )


def check_semantic_dimensions_meta_keys(
    models: Sequence[SemanticModel],
    meta_keys: Sequence[str],
    allow_extra_keys: bool,
) -> int:
    """
    Iterate over SemanticModel instances and validate that each dimension
    has the required meta keys in its config.meta dict.
    Returns 0 if all pass, 1 otherwise.
    """
    status_code = 0
    for model in models:
        for dim in model.dimensions:
            dim_name = dim.get("name", "<unnamed>")
            # Extract meta dict under config
            meta = dim.get("config", {}).get("meta", {})
            if not validate_meta_keys(meta.keys(), meta_keys, allow_extra_keys):
                status_code = 1
                print(
                    f"{red(model.name)}: dimension '{dim_name}' is missing or has extra meta keys"
                )
                print(f"  - {yellow('Expected: ' + ', '.join(meta_keys))}")
                print(f"  - {yellow('Actual: ' + ', '.join(meta.keys()))}")
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entrypoint: parse arguments, load dbt manifests, extract SemanticModel
    objects, and perform the meta key validation.
    """
    parser = argparse.ArgumentParser(
        description="Validate semantic dimension meta keys"
    )
    add_default_args(parser)
    add_meta_keys_args(parser)
    args = parser.parse_args(argv)

    # Load semantic manifest JSON
    try:
        semantic_manifest = get_dbt_semantic_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load semantic manifest file ({e})")
        return 1

    # Load dbt manifest for tracking context
    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    # Extract dataclass objects
    models = list(get_semantic_models(semantic_manifest))

    start_time = time.time()
    status_code = check_semantic_dimensions_meta_keys(
        models,
        meta_keys=args.meta_keys,
        allow_extra_keys=args.allow_extra_keys,
    )
    end_time = time.time()

    # Track hook execution
    script_args = vars(args)
    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check semantic model dimensions have required meta keys",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
