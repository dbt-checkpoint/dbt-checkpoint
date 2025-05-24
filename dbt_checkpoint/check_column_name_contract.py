import argparse
import os
import re
import time
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import add_catalog_args
from dbt_checkpoint.utils import add_default_args
from dbt_checkpoint.utils import get_dbt_catalog
from dbt_checkpoint.utils import get_dbt_manifest
from dbt_checkpoint.utils import get_filenames
from dbt_checkpoint.utils import get_missing_file_paths
from dbt_checkpoint.utils import get_models
from dbt_checkpoint.utils import JsonOpenError
from dbt_checkpoint.utils import red
from dbt_checkpoint.utils import yellow


def check_column_name_contract(
    paths: Sequence[str],
    pattern: str,
    dtypes: Sequence[str],
    catalog: Dict[str, Any],
    manifest: Dict[str, Any],
    exclude_pattern: str,
    include_disabled: bool,
) -> Dict[str, Any]:
    missing_file_paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
    )

    status_code = 0
    sqls = get_filenames(missing_file_paths, [".sql"])
    filenames = set(sqls.keys())
    models = get_models(catalog, filenames, include_disabled=include_disabled)

    for model in models:
        for col in model.node.get("columns", {}).values():
            col_name = col.get("name")
            col_type = col.get("type")

            # Check all files on dtypes follow naming pattern
            if any(col_type.lower() == dtype.lower() for dtype in dtypes):
                if re.match(pattern, col_name, re.IGNORECASE) is None:
                    status_code = 1
                    print(
                        f"model {red(model.model_id)}, in file"
                        f" {yellow(model.filename + '.sql')} \n"
                        f"{yellow(col_name)}: column is of type"
                        f" {yellow(col_type)} and does not match"
                        f" regex pattern {yellow(pattern)}."
                    )

            # Check all files with naming pattern are one of dtypes
            elif re.match(pattern, col_name, re.IGNORECASE):
                status_code = 1
                print(
                    f"model {red(model.model_id)}, in file"
                    f" {yellow(model.filename + '.sql')}"
                    f"\n{yellow(col_name)}: name matches"
                    f" regex pattern {yellow(pattern)} "
                    f"and is of type {yellow(col_type)}"
                    f" instead of {yellow(', '.join(dtypes))}."
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
    status_code = hook_properties["status_code"]
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check column name abides to contract.",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
