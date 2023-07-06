import argparse
import itertools
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Generator, Optional, Sequence, Set, Tuple

from dbt_checkpoint.check_script_has_no_table_name import has_table_name
from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import JsonOpenError, add_default_args, get_dbt_manifest


def get_ref_from_name(
    manifest: Dict[str, Any], tables: Set[str]
) -> Generator[Tuple[str, str], None, None]:
    table_names = {table.split(".")[-1]: table for table in tables}
    models = manifest.get("nodes", {})
    for _, value in models.items():
        # model name has to be unique
        model_name = value.get("alias")
        table = table_names.pop(model_name, None)
        if table:
            tables.remove(table)
            model_ref = "{{ ref('%s') }}" % model_name
            yield (table, model_ref)


def get_source_from_name(
    manifest: Dict[str, Any], tables: Set[str]
) -> Generator[Tuple[str, str], None, None]:
    if tables:
        table_names = {table: set(table.split(".")) for table in tables}
        sources = manifest.get("sources", {})
        for _, value in sources.items():
            source = {value.get("database"), value.get("schema"), value.get("name")}
            table = None  # pragma: no mutate
            for table_name, table_split in table_names.items():
                if source.issuperset(table_split):
                    table = table_name
            if table:
                tables.remove(table)
                source_ref = "{{ source('%s', '%s') }}" % (
                    value.get("source_name"),
                    value.get("name"),
                )
                yield (table, source_ref)


def get_unknown_source(tables: Set[str]) -> Generator[Tuple[str, str], None, None]:
    for table in tables:
        table_split = table.split(".")
        if len(table_split) > 1:
            source_name = table_split[-2]
            table_name = table_split[-1]
            print(
                f"Unable to find {table} in models or sources. "
                f"It probably means that does not exists. Trying "
                f"to replace {table} with source('{source_name}', "
                f"'{table_name}')"
            )
            source_ref = "{{ source('%s', '%s') }}" % (source_name, table_name)
            yield (table, source_ref)
        else:
            print(f"Unable to replace table {table} with ref or source.")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    status_code = 0

    start_time = time.time()
    for filename in args.filenames:
        file = Path(filename)
        sql = file.read_text()
        status_code_file, tables = has_table_name(sql, filename)
        if status_code_file:
            status_code = status_code_file
            to_replace = itertools.chain(
                get_ref_from_name(manifest, tables),
                get_source_from_name(manifest, tables),
                get_unknown_source(tables),
            )
            for replacement in to_replace:
                old = r"([\\\s\n\r\t])" + replacement[0] + r"([\\\s\n\r\t])"
                new = r"\1" + replacement[1] + r"\2"
                sql = re.sub(old, new, sql, re.IGNORECASE)
            file.write_text(sql, encoding="utf-8")
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Replace table names with source() or ref() macros in the script.",  # noqa: E501, line length
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
