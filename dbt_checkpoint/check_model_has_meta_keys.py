import argparse
import os
import time
from typing import Any, Dict, Iterable, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    add_meta_keys_args,
    get_dbt_manifest,
    get_filenames,
    get_model_schemas,
    get_model_sqls,
    get_models,
)


def validate_keys(
    actual: Iterable[str], expected: Iterable[str], allow_extra_keys: bool
) -> bool:
    actual = set(actual)
    expected = set(expected)

    if allow_extra_keys:
        return expected.issubset(actual)
    else:
        return expected == actual


def has_meta_key(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    meta_keys: Sequence[str],
    allow_extra_keys: bool,
    include_disabled: bool = False,
) -> int:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())
    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames, include_disabled=include_disabled)
    # if user added schema but did not rerun the model
    schemas = get_model_schemas(list(ymls.values()), filenames)
    # convert to sets
    in_models = {
        model.filename
        for model in models
        if validate_keys(model.node.get("meta", {}).keys(), meta_keys, allow_extra_keys)
    }
    in_schemas = {
        schema.model_name
        for schema in schemas
        if validate_keys(
            schema.schema.get("meta", {}).keys(), meta_keys, allow_extra_keys
        )
    }
    missing = filenames.difference(in_models, in_schemas)

    for model in missing:
        status_code = 1
        result = "\n- ".join(list(meta_keys))  # pragma: no mutate
        print(
            f"{sqls.get(model)}: "
            f"does not have some of the meta keys defined:\n- {result}",
        )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    add_meta_keys_args(parser)
    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code = has_meta_key(
        paths=args.filenames,
        manifest=manifest,
        meta_keys=args.meta_keys,
        allow_extra_keys=args.allow_extra_keys,
        include_disabled=args.include_disabled,
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check model has meta keys",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
