import argparse
import itertools
import os
import time
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import add_default_args
from dbt_checkpoint.utils import add_meta_keys_args
from dbt_checkpoint.utils import get_dbt_manifest
from dbt_checkpoint.utils import get_filenames
from dbt_checkpoint.utils import get_model_schemas
from dbt_checkpoint.utils import get_model_sqls
from dbt_checkpoint.utils import get_models
from dbt_checkpoint.utils import JsonOpenError
from dbt_checkpoint.utils import Model
from dbt_checkpoint.utils import ModelSchema
from dbt_checkpoint.utils import red
from dbt_checkpoint.utils import yellow


def validate_meta_keys(
    meta: Sequence[str],
    meta_set: Set[str],
    allow_extra_keys: bool,
) -> int:
    if allow_extra_keys:
        diff = not meta_set.issubset(meta)
    else:
        diff = not (meta_set == meta)
    if diff:
        return 0
    return 1


def missing_or_extra_meta_keys_dict(
    meta: Sequence[str], meta_set: Set[str]
) -> Dict[str, Sequence[str]]:
    return {
        "missing_meta_keys": sorted(
            [meta_key for meta_key in meta_set if meta_key not in meta]
        ),
        "extra_meta_keys": sorted(
            [
                extra_meta_key
                for extra_meta_key in meta
                if extra_meta_key not in meta_set
            ]
        ),
    }


def check_column_has_meta_keys(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    meta_keys: Sequence[str],
    allow_extra_keys: bool,
    include_disabled: bool = False,
) -> Tuple[int, Dict[Optional[str], Any]]:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames, include_disabled=include_disabled)
    # if user added schema but did not rerun the model
    schemas = get_model_schemas(list(ymls.values()), filenames)
    missing: Dict[Optional[str], Any] = {}
    meta_set = set(meta_keys)

    for item in itertools.chain(models, schemas):
        missing_cols = {}
        model_name = None
        if isinstance(item, ModelSchema):
            model_name = item.model_name
            missing_cols = {
                columns.get("name"): missing_or_extra_meta_keys_dict(
                    columns.get("meta", {}).keys(), meta_set
                )
                for columns in item.schema.get("columns", [])
                if not validate_meta_keys(
                    columns.get("meta", {}).keys(), meta_set, allow_extra_keys
                )
            }
        # Model
        elif isinstance(item, Model):
            model_name = item.filename
            missing_cols = {
                column_name: missing_or_extra_meta_keys_dict(
                    config.get("meta", {}).keys(), meta_set
                )
                for column_name, config in item.node.get("columns", {}).items()
                if not validate_meta_keys(
                    config.get("meta", {}).keys(), meta_set, allow_extra_keys
                )
            }

        for key, value in missing_cols.items():
            missing[model_name] = missing.get(model_name, {})
            missing[model_name][key] = value

    for model, columns in missing.items():
        if columns:
            status_code = 1
            result = ""
            for column, missing_or_extra_meta_keys in columns.items():
                result += f"\n- name: {yellow(column)}"
                if len(missing_or_extra_meta_keys.get("missing_meta_keys", [])) > 0:
                    result += "\n  missing keys:"
                    for meta_key in missing_or_extra_meta_keys.get(
                        "missing_meta_keys", []
                    ):
                        result += f"\n  - {yellow(meta_key)}"
                if (
                    not allow_extra_keys
                    and len(missing_or_extra_meta_keys.get("extra_meta_keys", [])) > 0
                ):
                    result += "\n  unknown extra keys:"
                    for meta_key in missing_or_extra_meta_keys.get(
                        "extra_meta_keys", []
                    ):
                        result += f"\n  - {yellow(meta_key)}"

            print(
                f"{red(sqls.get(model))}: "
                "following columns do not have all of the meta keys "
                "defined and/or unknown keys:"
                f"\n {result}",
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
    status_code, _ = check_column_has_meta_keys(
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
