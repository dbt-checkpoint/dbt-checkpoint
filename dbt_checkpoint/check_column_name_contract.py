import argparse
import os
import re
import time
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_catalog_args,
    add_default_args,
    get_dbt_catalog,
    get_dbt_manifest,
    get_filenames,
    get_missing_file_paths,
    get_models,
    red,
    yellow,
)


def check_column_name_contract(
    paths: Sequence[str],
    column_pattern: str,
    dtype_pattern: str,
    catalog: Dict[str, Any],
    manifest: Dict[str, Any],
    exclude_pattern: str,
) -> Dict[str, Any]:
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
    )

    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    models = get_models(catalog, filenames)

    for model in models:
        for col in model.node.get("columns", []).values():
            col_name = col.get("name")
            col_type = col.get("type")

            # Check all files of type dtype follow naming pattern
            if re.match(dtype_pattern, col_type) is Not None:
                if re.match(column_pattern, col_name) is None:
                    status_code = 1
                    print(
                        f"{red(col_name)}: column is of type {yellow(dtype_pattern)} and "
                        f"does not match regex pattern {yellow(column_pattern)}."
                    )

            # Check all files with naming pattern are of type dtype
            elif re.match(column_pattern, col_name):
                status_code = 1
                print(
                    f"{red(col_name)}: name matches regex pattern {yellow(column_pattern)} "
                    f"and is of type {yellow(col_type)} instead of {yellow(dtype_pattern)}."
                )

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    add_catalog_args(parser)

    parser.add_argument(
        "--column_pattern",
        type=str,
        required=True,
        help="Regex pattern to match column names.",
    )
    parser.add_argument(
        "--dtype_pattern",
        type=str,
        required=True,
        help="Expected data type for the matching columns.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    try:
        catalog = get_dbt_catalog(args)
    except JsonOpenError as e:
        print(f"Unable to load catalog file ({e})")
        return 1

    start_time = time.time()
    hook_properties = check_column_name_contract(
        paths=args.filenames,
        column_pattern=args.column_pattern,
        dtype_pattern=args.dtype_pattern,
        catalog=catalog,
        manifest=manifest,
        exclude_pattern=args.exclude,
    )

    end_time = time.time()

    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check column name abides to contract.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
