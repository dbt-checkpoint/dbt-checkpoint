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
    pattern: str,
    dtypes: Sequence[str],
    catalog: Dict[str, Any],
    manifest: Dict[str, Any],
    exclude_pattern: str,
    include_disabled: bool,
) -> Dict[str, Any]:
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
    )

    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    models = get_models(catalog, filenames, include_disabled=include_disabled)

    for model in models:
        for col in model.node.get("columns", []).values():
            col_name = col.get("name")
            col_type = col.get("type")

            # Check all files on dtypes follow naming pattern
            if any(col_type.lower() == dtype.lower() for dtype in dtypes):
                if re.match(pattern, col_name, re.IGNORECASE) is None:
                    status_code = 1
                    print(
                        f"model {red(model.model_id)}, in file {yellow(model.filename + '.sql')} \n"
                        f"{yellow(col_name)}: column is of type {yellow(col_type)} and "
                        f"does not match regex pattern {yellow(pattern)}."
                    )

            # Check all files with naming pattern are one of dtypes
            elif re.match(pattern, col_name, re.IGNORECASE):
                status_code = 1
                print(
                    f"model {red(model.model_id)}, in file {yellow(model.filename + '.sql')} \n"
                    f"{yellow(col_name)}: name matches regex pattern {yellow(pattern)} "
                    f"and is of type {yellow(col_type)} instead of {yellow(', '.join(dtypes))}."
                )

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    add_catalog_args(parser)

    parser.add_argument(
        "--pattern",
        type=str,
        required=True,
        help="Regex pattern to match column names.",
    )
    parser.add_argument(
        "--dtypes",
        nargs="+",
        required=True,
        help="Expected data types for the matching columns.",
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
        pattern=args.pattern,
        dtypes=args.dtypes,
        catalog=catalog,
        manifest=manifest,
        exclude_pattern=args.exclude,
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
            "description": "Check column name abides to contract.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
