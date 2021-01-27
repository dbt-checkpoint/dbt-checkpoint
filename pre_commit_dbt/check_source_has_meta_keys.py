import argparse
from pathlib import Path
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import get_source_schemas


def has_meta_key(paths: Sequence[str], meta_keys: Sequence[str]) -> int:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        schema_meta = set(schema.source_schema.get("meta", {}).keys())
        table_meta = set(schema.table_schema.get("meta", {}).keys())
        diff = set(meta_keys).difference(schema_meta, table_meta)
        if diff:
            status_code = 1
            result = "\n- ".join(list(meta_keys))  # pragma: no mutate
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"does not have some of the meta keys defined:\n- {result}",
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)

    parser.add_argument(
        "--meta-keys",
        nargs="+",
        required=True,
        help="List of required key in meta part of source.",
    )

    args = parser.parse_args(argv)

    return has_meta_key(paths=args.filenames, meta_keys=args.meta_keys)


if __name__ == "__main__":
    exit(main())
