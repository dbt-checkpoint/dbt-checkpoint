import argparse
import os
import time
from typing import Any, Dict, Iterable, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
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


def has_labels_key(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    labels_keys: Sequence[str],
    allow_extra_keys: bool,
) -> int:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())
    models = get_models(manifest, filenames)
    schemas = get_model_schemas(list(ymls.values()), filenames)

    in_models = set()
    for model in models:
        model_config = model.node.get("config", {})
        model_labels = set(model_config.get("labels", {}).keys())
        if validate_keys(model_labels, labels_keys, allow_extra_keys):
            in_models.add(model.filename)

    in_schemas = set()
    for schema in schemas:
        schema_config = schema.schema.get("config", {})
        schema_labels = set(schema_config.get("labels", {}).keys())

        if validate_keys(schema_labels, labels_keys, allow_extra_keys):
            in_schemas.add(schema.model_name)

    missing = filenames.difference(in_models, in_schemas)

    for model in missing:
        status_code = 1
        result = "\n- ".join(list(labels_keys))
        print(
            f"{sqls.get(model)}: "
            f"does not have some of the labels keys defined:\n- {result}",
        )

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--labels-keys",
        nargs="+",
        required=True,
        help="List of required key in labels part of model.",
    )

    parser.add_argument(
        "--allow-extra-keys",
        action="store_true",
        required=False,
        help="Whether extra keys are allowed.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code = has_labels_key(
        paths=args.filenames,
        manifest=manifest,
        labels_keys=args.labels_keys,
        allow_extra_keys=args.allow_extra_keys,
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check model has labels keys",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
