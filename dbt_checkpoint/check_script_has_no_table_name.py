import argparse
import os
import re
import time
from pathlib import Path
from typing import Generator, Optional, Sequence, Set, Tuple

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    red,
    yellow,
)

REGEX_COMMENTS = r"(?<=(\/\*|\{#))((.|[\r\n])+?)(?=(\*+\/|#\}))|[ \t]*--.*"
REGEX_SPLIT = r"[\s]+"
IGNORE_WORDS = ["", "(", "{{", "{"]  # pragma: no mutate
REGEX_PARENTHESIS = r"([\(\)])"  # pragma: no mutate
REGEX_BRACES = r"([\{\}])"  # pragma: no mutate


def prev_cur_next_iter(
    sql: Sequence[str],
) -> Generator[Tuple[Optional[str], str, Optional[str]], None, None]:
    sql_iter = iter(sql)
    prev = None
    cur = next(sql_iter).lower()
    try:
        while True:
            nxt = next(sql_iter).lower()  # pragma: no mutate
            yield prev, cur, nxt
            prev = cur
            cur = nxt
    except StopIteration:
        yield prev, cur, None


def replace_comments(sql: str) -> str:
    return re.sub(REGEX_COMMENTS, "", sql)


def add_space_to_parenthesis(sql: str) -> str:
    return re.sub(REGEX_PARENTHESIS, r" \1 ", sql)


def add_space_to_braces(sql: str) -> str:
    return re.sub(REGEX_BRACES, r" \1 ", sql)


def add_space_to_source_ref(sql: str) -> str:
    return sql.replace("{{", "{{ ").replace("}}", " }}")


def has_table_name(
    sql: str, filename: str, dotless: Optional[bool] = False
) -> Tuple[int, Set[str]]:
    status_code = 0
    sql_clean = replace_comments(sql)
    sql_clean = add_space_to_parenthesis(sql_clean)
    sql_clean = add_space_to_braces(sql_clean)
    sql_clean = add_space_to_source_ref(sql_clean)
    sql_split = re.split(REGEX_SPLIT, sql_clean)
    tables = set()
    cte = set()

    for prev, cur, nxt in prev_cur_next_iter(sql_split):
        if prev in ["from", "join"] and cur not in IGNORE_WORDS:
            table = cur.lower().strip().replace(",", "") if cur else cur
            if dotless and "." not in table:
                pass
            else:
                tables.add(table)
        if (
            cur.lower() == "as" and nxt and nxt[0] == "(" and prev not in IGNORE_WORDS
        ):  # pragma: no mutate
            cte.add(prev.lower() if prev else prev)

    table_names = tables.difference(cte)
    if table_names:
        status_code = 1
    return status_code, table_names


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument("--ignore-dotless-table", action="store_true")

    args = parser.parse_args(argv)
    status_code = 0

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    script_args = vars(args)

    start_time = time.time()
    for filename in args.filenames:
        sql = Path(filename).read_text()
        status_code_file, tables = has_table_name(
            sql, filename, args.ignore_dotless_table
        )
        if status_code_file:
            result = "\n- ".join(list(tables))  # pragma: no mutate
            print(
                f"{red(filename)}: "
                f"does not use source() or ref() macros for tables:\n",
                f"- {yellow(result)}",
            )
            status_code = status_code_file

    end_time = time.time()

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the script has no table name (is not using source() or ref() macro for all tables).",  # pragma: no mutate
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
