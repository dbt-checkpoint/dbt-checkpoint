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

# Type equivalence mapping - groups of types that should be treated as equivalent
TYPE_EQUIVALENCE_GROUPS = [
    # String types
    {"STRING", "TEXT", "VARCHAR", "CHAR", "CHARACTER VARYING", "NVARCHAR", "NCHAR"},
    # Integer types
    {"INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "INT64", "INT8", "INT4", "INT2"},
    # Decimal/Numeric types
    {"DECIMAL", "NUMERIC", "NUMBER"},
    # Float types
    {"FLOAT", "DOUBLE", "REAL", "FLOAT64", "DOUBLE PRECISION"},
    # Boolean types
    {"BOOLEAN", "BOOL"},
    # Timestamp types
    {"TIMESTAMP_TZ", "TIMESTAMP"},
    # Date types
    {"DATE"},
    # Time types
    {"TIME"},
]


def normalize_type(type_str: str) -> str:
    """
    Normalize a type string to its canonical form for comparison.
    Returns the first (canonical) type from the equivalence group, or the original type if no match.
    """
    type_upper = type_str.upper().strip()

    for group in TYPE_EQUIVALENCE_GROUPS:
        if type_upper in group:
            # Return the first element (canonical type) from the group
            return sorted(group)[0]

    return type_upper


def check_required_columns(
    catalog_columns: Dict[str, Any], required_columns: List[Dict[str, str]]
) -> Tuple[List[str], List[str]]:
    missing_columns = []
    wrong_type_columns = []

    catalog_cols_lower = {col.lower(): col_info for col, col_info in catalog_columns.items()}

    for req_col in required_columns:
        col_name = req_col["name"].lower()
        expected_type = normalize_type(req_col["type"])

        if col_name not in catalog_cols_lower:
            missing_columns.append(f"{req_col['name']} ({req_col['type']})")
        else:
            actual_type_raw = catalog_cols_lower[col_name].get("type", "")
            actual_type = normalize_type(actual_type_raw)

            if actual_type != expected_type:
                wrong_type_columns.append(
                    f"{req_col['name']} (expected: {req_col['type']}, actual: {actual_type_raw})"
                )

    return missing_columns, wrong_type_columns


def check_model_columns_with_types(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    catalog: Dict[str, Any],
    required_columns: List[Dict[str, str]],
    exclude_pattern: str,
    include_disabled: bool = False,
) -> int:
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
            missing_columns, wrong_type_columns = check_required_columns(
                catalog_columns=catalog_node.get("columns", {}),
                required_columns=required_columns,
            )
            
            if missing_columns or wrong_type_columns:
                status_code = 1
                model_file = red(sqls.get(model.filename))
                print(f"Model {model_file} has column issues:")
                
                if missing_columns:
                    print(f"  Missing required columns:")
                    for col in missing_columns:
                        print(f"    - {yellow(col)}")
                
                if wrong_type_columns:
                    print(f"  Columns with incorrect types:")
                    for col in wrong_type_columns:
                        print(f"    - {red(col)}")
                print()
        else:
            status_code = 1
            print(
                f"Unable to find model `{red(model.model_id)}` in catalog file. "
                f"Make sure you run `dbt docs generate` before executing this hook."
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    add_catalog_args(parser)
    parser.add_argument(
        "--columns",
        required=True,
        help='Required columns with types in JSON format. Example: [{"name": "_loaded_at", "type": "timestamp"}, {"name": "_updated_at", "type": "timestamp"}]',
        metavar='[{"name": "_loaded_at", "type": "timestamp"}]',
        dest="columns",
        type=str,
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

    try:
        import json
        required_columns = json.loads(args.columns)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON in --columns argument: {e}")
        return 1

    if not isinstance(required_columns, list):
        print("--columns argument must be a JSON array")
        return 1

    for col in required_columns:
        if not isinstance(col, dict) or "name" not in col or "type" not in col:
            print("Each column in --columns must be an object with 'name' and 'type' keys")
            return 1

    start_time = time.time()
    status_code = check_model_columns_with_types(
        paths=args.filenames,
        manifest=manifest,
        catalog=catalog,
        required_columns=required_columns,
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
            "description": "Check model has columns with specified types",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())