import argparse
import itertools
import os
import time
from typing import Any, Dict, Optional, Sequence, Set, Tuple, Iterable

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
    validate_meta_keys,
    add_meta_keys_args,
    red,
    yellow,
)

def validate_meta_keys(
    meta: Sequence[str],
    meta_set: Set,
    allow_extra_keys: bool,
):
    if allow_extra_keys:
        diff = not meta_set.issubset(meta)
    else:
        diff = not (meta_set == meta)
    if diff:
        return 0
    return 1

def check_column_has_meta_keys(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    meta_keys: Sequence[str],
    allow_extra_keys: bool,
    include_disabled: bool = False
) -> Tuple[int, Dict[str, Any]]:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames, include_disabled=include_disabled)
    # if user added schema but did not rerun the model
    schemas = get_model_schemas(list(ymls.values()), filenames)
    missing: Dict[str, Set[str]] = {}
    meta_set = set(meta_keys)

    for item in itertools.chain(models, schemas):
        missing_cols = set()  # pragma: no mutate
        if isinstance(item, ModelSchema):
            model_name = item.model_name
            missing_cols = {
                key.get("name"):[meta_key for meta_key in list(meta_set) if meta_key not in key.get("meta", {}).keys()]
                for key in item.schema.get("columns", [])
                if not validate_meta_keys(key.get("meta", {}).keys(), meta_set, allow_extra_keys)
            }
        # Model
        elif isinstance(item, Model):
            model_name = item.filename
            
            missing_cols = {
                key: [meta_key for meta_key in list(meta_set) if meta_key not in value.get("meta", {}).keys()]
                for key, value in item.node.get("columns", {}).items()
                if (
                    not value.get("meta") or 
                    not validate_meta_keys(value.get("meta", {}).keys(), meta_set, allow_extra_keys) if value.get("meta") else False
                )
            }
        else:
            continue
        seen = missing.get(model_name)
        if seen:
            if not missing_cols:
                missing[model_name] = set()  # pragma: no mutate
            else:
                missing[model_name] = seen.union(missing_cols)
        elif missing_cols:
            missing[model_name] = missing_cols
    for model, columns in missing.items():
        if columns:
            status_code = 1
            result = ""  # pragma: no mutate
            for column, missing_meta_keys in columns.items():
                result += f"\n- {column}"  # pragma: no mutate
                for meta_key in missing_meta_keys:
                    result += f"\n  - {meta_key}"  # pragma: no mutate

            print(
                f"{red(sqls.get(model))}: "
                f"following columns do not have all of the meta keys defined:\n {yellow(result)}",
            )
    return status_code, missing


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
    status_code, missing = check_column_has_meta_keys(
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