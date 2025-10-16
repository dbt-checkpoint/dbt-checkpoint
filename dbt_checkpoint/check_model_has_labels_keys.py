import argparse
import itertools
import os
import time
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    Model,
    ModelSchema,
    add_default_args,
    get_dbt_manifest,
    get_filenames,
    get_model_schemas,
    get_model_sqls,
    get_models,
    red,
    yellow,
)


def has_labels_key(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    labels_keys: Sequence[str],
    allow_extra_keys: bool,
    include_disabled: bool = False,
) -> int:
    status_code = 0
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())
    ymls = get_filenames(paths, [".yml", ".yaml"])

    models = get_models(manifest, filenames, include_disabled=include_disabled)
    schemas = get_model_schemas(list(ymls.values()), filenames)

    for item in itertools.chain(models, schemas):
        if isinstance(item, ModelSchema): # pragma: no cover
            model_name = item.model_name
            config = item.schema.get("config", {})
        elif isinstance(item, Model):
            model_name = item.filename
            config = item.node.get("config", {})
        else:
            continue  # pragma: no cover

        labels = config.get("labels", {})
        
        if not isinstance(labels, dict):
            print(
                f"{sqls.get(model_name)}: `labels` is not a dictionary."
            )
            status_code = 1
            continue

        if allow_extra_keys:
            diff = set(labels_keys).difference(labels.keys())
        else:
            diff = set(labels_keys).symmetric_difference(labels.keys())

        if diff:
            status_code = 1
            print(
                f"{sqls.get(model_name)}: "
                f"does not have some of the labels keys defined: "
                f"{yellow(list(diff))}",
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    # Replace the non-existent function with direct argument definitions
    parser.add_argument(
        "--labels-keys",
        nargs="+",
        required=True,
        help="List of required label keys.",
    )
    parser.add_argument(
        "--no-allow-extra-keys",
        action="store_false",
        dest="allow_extra_keys",
        help="If passed, model labels must match exactly.",
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
            "description": "Check model has labels keys",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())