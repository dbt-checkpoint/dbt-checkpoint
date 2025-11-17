import argparse
import os
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_catalog_args,
    add_default_args,
    get_dbt_catalog,
    get_dbt_manifest,
    get_missing_file_paths,
    get_model_sqls,
    get_models,
    red,
    yellow,
)


def check_blacklisted_types(
    catalog_columns: Dict[str, Any], blacklisted_types: List[str]
) -> List[Tuple[str, str]]:
    """
    Check if any columns have blacklisted types.

    Args:
        catalog_columns: Dictionary of columns from catalog
        blacklisted_types: List of type names that are not allowed

    Returns:
        List of tuples (column_name, column_type) for columns with blacklisted types
    """
    blacklisted_columns = []

    # Convert blacklisted types to uppercase for case-insensitive comparison
    blacklisted_types_upper = [t.upper() for t in blacklisted_types]

    for col_name, col_info in catalog_columns.items():
        col_type = col_info.get("type", "").upper()

        if col_type in blacklisted_types_upper:
            blacklisted_columns.append((col_name, col_type))

    return blacklisted_columns


def check_model_column_type_blacklist(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    catalog: Dict[str, Any],
    blacklisted_types: List[str],
    exclude_pattern: str,
    include_disabled: bool = False,
) -> int:
    """
    Check models for columns with blacklisted types.

    Args:
        paths: List of file paths to check
        manifest: dbt manifest dictionary
        catalog: dbt catalog dictionary
        blacklisted_types: List of type names that are not allowed
        exclude_pattern: Pattern to exclude files
        include_disabled: Whether to include disabled models

    Returns:
        Status code (0 = success, 1 = failure)
    """
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
    )

    status_code = 0
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())

    models = get_models(manifest, filenames, include_disabled=include_disabled)

    catalog_nodes = catalog.get("nodes", {})

    for model in models:
        catalog_node = catalog_nodes.get(model.model_id, {})
        if catalog_node:
            blacklisted_columns = check_blacklisted_types(
                catalog_columns=catalog_node.get("columns", {}),
                blacklisted_types=blacklisted_types,
            )

            if blacklisted_columns:
                status_code = 1
                model_file = red(sqls.get(model.filename))
                print(f"Model {model_file} has columns with blacklisted types:")

                for col_name, col_type in blacklisted_columns:
                    print(f"  - {yellow(col_name)}: {red(col_type)}")
                print()
        else:
            status_code = 1
            print(
                f"Unable to find model `{red(model.model_id)}` in catalog file. "
                f"Make sure you run `dbt docs generate` before executing this hook."
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check that models do not contain columns with blacklisted types."
    )
    add_default_args(parser)
    add_catalog_args(parser)
    parser.add_argument(
        "--types",
        required=True,
        nargs="+",
        help="List of column types that are not allowed (e.g., timestamp_ntz timestamp_ltz)",
        metavar="TYPE",
        dest="types",
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
    status_code = check_model_column_type_blacklist(
        paths=args.filenames,
        manifest=manifest,
        catalog=catalog,
        blacklisted_types=args.types,
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
            "description": "Check column type blacklist",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())