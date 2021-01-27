import argparse
from pathlib import Path
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import get_source_schemas


def check_column_desc(paths: Sequence[str]) -> int:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        missing_cols = {
            col.get("name")
            for col in schema.table_schema.get("columns", [])
            if not col.get("description")
        }
        if missing_cols and all(missing_cols):
            status_code = 1
            result = "\n- ".join(list(missing_cols))  # pragma: no mutate
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"following columns are missing description:\n- {result}",
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)

    args = parser.parse_args(argv)

    return check_column_desc(paths=args.filenames)


if __name__ == "__main__":
    exit(main())
