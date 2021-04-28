import argparse
import re
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_catalog_args
from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import get_filenames
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import JsonOpenError


def check_column_name_contract(
    paths: Sequence[str], pattern: str, dtype: str, catalog: Dict[str, Any]
) -> int:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    models = get_models(catalog, filenames)

    for model in models:
        for col in model.node.get("columns", []).values():
            col_name = col.get("name")
            col_type = col.get("type")

            # Check all files of type dtype follow naming pattern
            if dtype == col_type:
                if re.match(pattern, col_name) is None:
                    status_code = 1
                    print(
                        f"{col_name}: column is of type {dtype} and "
                        f"does not match regex pattern {pattern}."
                    )

            # Check all files with naming pattern are of type dtype
            elif re.match(pattern, col_name):
                status_code = 1
                print(
                    f"{col_name}: column name matches regex pattern {pattern} "
                    f"and is of type {col_type} instead of {dtype}."
                )

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_catalog_args(parser)

    parser.add_argument(
        "--pattern",
        type=str,
        required=True,
        help="Regex pattern to match column names.",
    )
    parser.add_argument(
        "--dtype",
        type=str,
        required=True,
        help="Expected data type for the matching columns.",
    )

    args = parser.parse_args(argv)

    try:
        catalog = get_json(args.catalog)
    except JsonOpenError as e:
        print(f"Unable to load catalog file ({e})")
        return 1

    return check_column_name_contract(
        paths=args.filenames,
        pattern=args.pattern,
        dtype=args.dtype,
        catalog=catalog,
    )


if __name__ == "__main__":
    exit(main())
