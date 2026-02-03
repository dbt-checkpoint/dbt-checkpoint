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
    get_config_file,
    get_dbt_catalog,
    get_dbt_manifest,
    get_filenames,
    get_missing_file_paths,
    get_models,
    red,
    yellow,
)

# A dictionary of default, common data type equivalencies across major data warehouses.
# Each key is also included in its set of values to ensure self-matching.
DEFAULT_TYPE_MAPPINGS = {
    "numeric": {
        "numeric", "decimal", "number", "float", "float64", "real", "double",
        "double precision", "bignumeric"
    },
    "string": {
        "string", "varchar", "char", "character", "text", "character varying"
    },
    "integer": {
        "integer", "int", "int64", "bigint", "smallint", "short", "long"
    },
    "boolean": {"boolean", "bool"},
    "datetime": {
        "datetime", "timestamp", "timestamptz", "timestamp_ntz", "timestamp_tz"
    },
    "complex": {
        "complex", "struct", "record", "array", "object", "variant", "json",
        "jsonb", "super", "hstore"
    },
    "geospatial": {"geospatial", "geography", "geometry"},
    "date": {"date"},
    "time": {"time"},
}


def get_base_type(col_type: str) -> str:
    """
    Extracts the base type from a potentially parameterized data type string.
    """
    if not col_type:
        return ""
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
    """
    status_code = 0
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
    )
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    
    # Correctly get models from the manifest first
    models = get_models(manifest, filenames, include_disabled=include_disabled)
    catalog_nodes = catalog.get("nodes", {})

    all_mappings = copy.deepcopy(DEFAULT_TYPE_MAPPINGS)
    hook_config = config.get("check-column-name-contract", {})
    user_mappings = hook_config.get("type_mappings", {})
    for key, value in user_mappings.items():
        key_lower = key.lower()
        if key_lower in all_mappings:
            all_mappings[key_lower].update([v.lower() for v in value])
        else:
            all_mappings[key_lower] = {v.lower() for v in value}

    for model in models:
        # Look up the corresponding model in the catalog to get column info
        catalog_node = catalog_nodes.get(model.model_id)
        if not catalog_node:
            continue

        for col in catalog_node.get("columns", {}).values():
            col_name = col.get("name")
            col_type_full = col.get("type")
            col_base_type = get_base_type(col_type_full)

            is_type_match = False
            for dtype in dtypes:
                dtype_lower = dtype.lower()
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
                        f"model {red(model.model_id)}, in file {yellow(sqls.get(model.filename))} \n"
                        f"{yellow(col_name)}: column is of type {yellow(col_type_full)} "
                        f"but does not match regex pattern {yellow(pattern)}."
                    )
            elif re.match(pattern, col_name, re.IGNORECASE):
                # The column name matches the pattern; now check if the type is correct.
                status_code = 1
                print(
                    f"model {red(model.model_id)}, in file {yellow(sqls.get(model.filename))} \n"
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