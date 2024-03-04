import argparse
import os
import time
from collections import Counter
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Any, Dict, Generator, Iterator, Optional, Sequence, Tuple

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    ModelSchema,
    add_default_args,
    get_dbt_manifest,
    get_filenames,
    get_model_schemas,
    red,
    yellow,
)


@dataclass
class ColumnDescription:
    column_name: str
    description: str
    file: Path
    new_description: Optional[str] = None  # pragma: no mutate


def get_all_columns(
    schemas: Generator[ModelSchema, None, None], ignore_list: Sequence[str]
) -> Generator[ColumnDescription, None, None]:
    for item in schemas:
        for column in item.schema.get("columns", {}):
            desc = column.get("description")
            column_name = column.get("name")
            if column_name not in ignore_list:
                yield ColumnDescription(column_name, desc, item.file)


def get_grouped(
    paths: Sequence[str], ignore: Optional[Sequence[str]]
) -> Iterator[Tuple[str, Iterator[ColumnDescription]]]:
    ignore_list = ignore or []
    ymls = get_filenames(paths, [".yml", ".yaml"])
    filenames = set(ymls.keys())

    schemas = get_model_schemas(list(ymls.values()), filenames, True)

    columns = get_all_columns(schemas, ignore_list)
    grouped = groupby(
        sorted(columns, key=lambda x: x.column_name), lambda x: x.column_name
    )
    return grouped


def check_column_desc(
    paths: Sequence[str], ignore: Optional[Sequence[str]]
) -> Dict[str, Any]:
    status_code = 0
    grouped = get_grouped(paths, ignore)

    for name, groups in grouped:
        group_cnt = Counter([group.description for group in groups])
        if len(group_cnt.keys()) > 1:
            status_code = 1
            print(f"{red(name)}: has different descriptions:")
            for desc, cnt in group_cnt.items():
                print(f"  - {yellow(cnt)}: {yellow(desc)}")
    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--ignore",
        nargs="*",
        help="Columns for which do not check whether have a different description.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = check_column_desc(paths=args.filenames, ignore=args.ignore)
    end_time = time.time()

    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check column descriptions are the same.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
