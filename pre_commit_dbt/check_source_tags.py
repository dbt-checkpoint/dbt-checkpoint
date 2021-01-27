import argparse
from pathlib import Path
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import get_source_schemas


def validate_tags(paths: Sequence[str], tags: Sequence[str]) -> int:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        schema_tags = set(schema.source_schema.get("tags", []))
        table_tags = set(schema.table_schema.get("tags", []))
        source_tags = schema_tags.union(table_tags)
        valid_tags = set(tags)
        if not source_tags.issubset(valid_tags):
            status_code = 1
            list_diff = list(source_tags.difference(valid_tags))
            result = "\n- ".join(list_diff)  # pragma: no mutate
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"has invalid tags:\n- {result}",
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)

    parser.add_argument(
        "--tags",
        nargs="+",
        required=True,
        help="A list of tags that source can have.",
    )

    args = parser.parse_args(argv)

    return validate_tags(paths=args.filenames, tags=args.tags)


if __name__ == "__main__":
    exit(main())
