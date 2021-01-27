import argparse
from pathlib import Path
from typing import Optional
from typing import Sequence
from typing import Set

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import get_source_schemas


def has_freshness(paths: Sequence[str], required_freshness: Set[str]) -> int:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        source = schema.source_schema
        table = schema.table_schema
        merged = {**source.get("freshness", {}), **table.get("freshness", {})}
        freshness = {
            key
            for key, value in merged.items()
            if set(value.keys()) == {"count", "period"}
        }
        loaded = table.get("loaded_at_field") or source.get("loaded_at_field")
        if not loaded:
            status_code = 1
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"is missing `loaded_at_field` which is required for freshness."
            )
        if not (freshness == required_freshness):
            status_code = 1
            missing_params = required_freshness.difference(freshness)
            result = "\n- ".join(list(missing_params))  # pragma: no mutate
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"miss some required freshness parameters:"
                f"\n- {result} "
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)

    parser.add_argument(
        "--freshness",
        nargs="+",
        required=True,
        choices=["warn_after", "error_after"],
        help="List of required freshness options.",
    )

    args = parser.parse_args(argv)

    return has_freshness(paths=args.filenames, required_freshness=set(args.freshness))


if __name__ == "__main__":
    exit(main())
