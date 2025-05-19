import argparse
import os
import time
import itertools
from typing import Any, Dict, Optional, Sequence, Set, Tuple, List

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_filenames,
    get_model_schemas,
    get_model_sqls,
    get_models,
    red,
    yellow,
    add_meta_keys_args,
    validate_column_meta_keys,
    Model,
    ModelSchema,
)


def check_column_meta_keys(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    meta_keys: Sequence[str],
    allow_extra_keys: bool,
    include_disabled: bool = False,
) -> Tuple[int, Dict[str, Any]]:
    """
    Checks that all model columns have the specified meta keys.

    Args:
        paths: List of file paths to check.
        manifest: The dbt manifest JSON.
        meta_keys: Required meta keys for columns.
        allow_extra_keys: If True, allows extra meta keys beyond the required ones.
        include_disabled: If True, includes disabled models.

    Returns:
        status_code: 0 if all columns have required meta keys, 1 otherwise.
        missing_meta: Dictionary of models with columns missing meta keys.
    """
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())

    models = get_models(manifest, filenames, include_disabled=include_disabled)
    schemas = get_model_schemas(list(ymls.values()), filenames)

    missing_meta: Dict[str, Dict[str, Set[str]]] = {}
    seen_models: Set[str] = set()  # Track models already checked in schema
    required_meta_keys = set(meta_keys)  # Convert required keys to a set

    # Iterate over both models (from SQL files) and schemas (from YAML files)
    for item in itertools.chain(schemas, models):
        if isinstance(item, ModelSchema):
            # If it's a schema file, get the model name and its columns
            model_name = item.model_name
            columns = item.schema.get("columns", [])
            seen_models.add(model_name)  # Mark model as checked in schema
        elif isinstance(item, Model):
            # If it's a dbt model, get the model name and its columns
            model_name = item.filename

            # Skip if this model was already checked in the schema
            if model_name in seen_models:
                continue

            columns = item.node.get("columns", {}).values()
        else:
            continue  # Skip if it's not a model or schema

        # Dictionary to track columns with missing meta keys
        missing_cols = {}

        for column in columns:
            col_name = column.get("name")  # Column name
            col_meta = column.get("meta", {})  # Column's meta data dictionary

            # Validate column meta keys using the new function
            is_valid = validate_column_meta_keys(
                meta=col_meta,
                required_meta_keys=required_meta_keys,
                allow_extra_keys=allow_extra_keys,
                model_name=model_name,
                column_name=col_name,
            )

            # If validation fails, track the column as missing meta keys
            if not is_valid:
                missing_keys = required_meta_keys - col_meta.keys()
                missing_cols[col_name] = set(missing_keys)

        # If any columns are missing required meta keys, update missing_meta
        if missing_cols:
            missing_meta[model_name] = missing_cols
            status_code = 1  # Indicate that at least one issue was found

    # Print missing meta keys for each model and column
    for model, columns in missing_meta.items():
        print(f"{red(sqls.get(model))}: Some columns are missing required meta keys:")
        for column, keys in columns.items():
            missing_list = ", ".join(keys)
            print(
                f"  - Column '{yellow(column)}' is missing meta keys: {yellow(missing_list)}"
            )

    return status_code, missing_meta


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
    status_code, _ = check_column_meta_keys(
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
            "description": "Check model columns have meta keys",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
