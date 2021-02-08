import argparse
import re
from pathlib import Path
from typing import Generator
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

from pre_commit_dbt.utils import add_filenames_args

REGEX_COMMENTS = r"(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/|[ \t]*--.*)"
REGEX_SPLIT = r"[\s]+"
IGNORE_WORDS = ["", "(", "{{"]  # pragma: no mutate


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


def has_table_name(sql: str, filename: str) -> Tuple[int, Set[str]]:
    status_code = 0
    sql_clean = replace_comments(sql)
    sql_split = re.split(REGEX_SPLIT, sql_clean)
    tables = set()
    cte = set()

    for prev, cur, nxt in prev_cur_next_iter(sql_split):
        if prev in ["from", "join"] and cur not in IGNORE_WORDS:
            tables.add(cur.lower() if cur else cur)
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
    add_filenames_args(parser)

    args = parser.parse_args(argv)
    status_code = 0

    for filename in args.filenames:
        sql = Path(filename).read_text()
        status_code_file, tables = has_table_name(sql, filename)
        if status_code_file:
            result = "\n- ".join(list(tables))  # pragma: no mutate
            print(
                f"{filename}: "
                f"does not use source() or ref() macros for tables:\n- {result}",
            )
            status_code = status_code_file

    return status_code


if __name__ == "__main__":
    exit(main())
