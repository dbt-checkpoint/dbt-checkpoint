import argparse
import itertools
import os
import time
from typing import Any, Dict, Optional, Sequence, Set, Tuple

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    Model,
    ModelSchema,
    add_default_args,
    get_dbt_manifest,
    get_filenames,
    get_missing_file_paths,
    get_model_schemas,
    get_model_sqls,
    get_models,
    red,
    yellow,
)


def check_column_desc(
    paths: Sequence[str], manifest: Dict[str, Any], include_disabled: bool = False
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

    for item in itertools.chain(models, schemas):
        missing_cols = set()  # pragma: no mutate
        if isinstance(item, ModelSchema):
            model_name = item.model_name
            missing_cols = {
                key.get("name")
                for key in item.schema.get("columns", [])
                if not key.get("description")
            }
        # Model
        elif isinstance(item, Model):
            model_name = item.filename
            missing_cols = {
                key
                for key, value in item.node.get("columns", {}).items()
                if (isinstance(value, dict) and not value.get("description"))
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
            result = "\n- ".join(list(columns))  # pragma: no mutate
            print(
                f"{red(sqls.get(model))}: "
                f"following columns are missing description:\n- {yellow(result)}",
            )
    return status_code, missing


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code, _ = check_column_desc(
        paths=args.filenames, manifest=manifest, include_disabled=args.include_disabled
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the model columns have description",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
