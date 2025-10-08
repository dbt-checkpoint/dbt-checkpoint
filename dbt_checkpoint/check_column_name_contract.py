import argparse
import copy
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
    get_config_file,
)

# A dictionary of default, common data type equivalencies across major data warehouses.
# This provides a sensible baseline for the hook's behavior.
DEFAULT_TYPE_MAPPINGS = {
    "numeric": {
        "decimal", "number", "float", "float64", "real", "double",
        "double precision", "bignumeric"
    },
    "string": {
        "varchar", "char", "character", "text", "character varying"
    },
    "integer": {
        "int", "int64", "bigint", "smallint", "short", "long"
    },
    "boolean": {"bool"},
    "datetime": {
        "timestamp", "timestamptz", "timestamp_ntz", "timestamp_tz"
    },
    "complex": {
        "struct", "record", "array", "object", "variant", "json", "jsonb",
        "super", "hstore"
    },
    "geospatial": {"geography", "geometry"},
}


def get_base_type(col_type: str) -> str:
    """
    Extracts the base type from a potentially parameterized data type string.

    For example, for inputs like 'ARRAY<STRING>' or 'STRUCT<field1 INT64>',
    this function will return 'array' and 'struct' respectively. For simple
    types like 'STRING', it will return 'string'.

    Args:
        col_type (str): The full data type string from the catalog.

    Returns:
        str: The lowercased base data type.
    """
    if not col_type:
        return ""
    # This regex matches the first word (the base type) of the data type string.
    match = re.match(r"([A-Z_]+)", col_type, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    return col_type.lower()


def check_column_name_contract(
    paths: Sequence[str],
    pattern: str,
    dtypes: Sequence[str],
    catalog: Dict[str, Any],
    manifest: Dict[str, Any],
    exclude_pattern: str,
    include_disabled: bool,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Checks if column names and types adhere to a defined contract.

    This function validates that:
    1. Columns of specified data types match the given regex pattern.
    2. Columns matching the regex pattern are of one of the specified data types.

    It uses a combination of default type equivalencies and user-defined
    mappings to handle variations in data types across different SQL databases.

    Args:
        paths (Sequence[str]): A list of file paths to check.
        pattern (str): The regex pattern for column names.
        dtypes (Sequence[str]): A list of allowed data types for the contract.
        catalog (Dict[str, Any]): The dbt catalog dictionary.
        manifest (Dict[str, Any]): The dbt manifest dictionary.
        exclude_pattern (str): A regex pattern to exclude files from checking.
        include_disabled (bool): Whether to include disabled models.
        config (Dict[str, Any]): The dbt-checkpoint configuration dictionary.

    Returns:
        Dict[str, Any]: A dictionary containing the status code of the check.
    """
    status_code = 0
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
    )
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    models = get_models(catalog, filenames, include_disabled=include_disabled)

    # 1. Start with the baked-in default type mappings.
    all_mappings = copy.deepcopy(DEFAULT_TYPE_MAPPINGS)

    # 2. Load and merge user-defined mappings from the config file.
    hook_config = config.get("check-column-name-contract", {})
    user_mappings = hook_config.get("type_mappings", {})
    for key, value in user_mappings.items():
        key_lower = key.lower()
        if key_lower in all_mappings:
            # If the key exists, update the set of equivalent types.
            all_mappings[key_lower].update([v.lower() for v in value])
        else:
            # If it's a new key, add it to the mappings.
            all_mappings[key_lower] = {v.lower() for v in value}

    for model in models:
        for col in model.node.get("columns", []).values():
            col_name = col.get("name")
            col_type_full = col.get("type")
            col_base_type = get_base_type(col_type_full)

            # Determine if the column's base type matches the contract's dtypes.
            is_type_match = False
            for dtype in dtypes:
                dtype_lower = dtype.lower()
                # Get the set of allowed types from the merged mappings.
                # Fall back to a set containing just the dtype itself if no mapping exists.
                allowed_types = all_mappings.get(dtype_lower, {dtype_lower})
                if col_base_type in allowed_types:
                    is_type_match = True
                    break

            # Perform the contract validation.
            if is_type_match:
                # The column type is correct; now check if the name follows the pattern.
                if re.match(pattern, col_name, re.IGNORECASE) is None:
                    status_code = 1
                    print(
                        f"model {red(model.model_id)}, in file {yellow(model.filename + '.sql')} \n"
                        f"{yellow(col_name)}: column is of type {yellow(col_type_full)} "
                        f"but does not match regex pattern {yellow(pattern)}."
                    )
            elif re.match(pattern, col_name, re.IGNORECASE):
                # The column name matches the pattern; now check if the type is correct.
                status_code = 1
                print(
                    f"model {red(model.model_id)}, in file {yellow(model.filename + '.sql')} \n"
                    f"{yellow(col_name)}: name matches regex pattern {yellow(pattern)} "
                    f"and is of type {yellow(col_type_full)} instead of {yellow(', '.join(dtypes))}."
                )

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    """The main entry point for the pre-commit hook."""
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

    # Load the dbt-checkpoint config file to get user-defined mappings.
    config = get_config_file(args.config)

    start_time = time.time()
    hook_properties = check_column_name_contract(
        paths=args.filenames,
        pattern=args.pattern,
        dtypes=args.dtypes,
        catalog=catalog,
        manifest=manifest,
        exclude_pattern=args.exclude,
        include_disabled=args.include_disabled,
        config=config,
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