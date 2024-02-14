import argparse
import re
from pathlib import Path
from typing import Optional
from typing import Sequence

from dbt_checkpoint.utils import (
    add_filenames_args,
    red,
)


REGEX_COMMENTS = r"(?<=(\/\*|\{#))((.|[\r\n])+?)(?=(\*+\/|#\}))|[ \t]*--.*"
REGEX_SPLIT = r"[\s]+"
REGEX_PARENTHESIS = r"([\(\)])"  # pragma: no mutate


def replace_comments(sql: str) -> str:
    return re.sub(REGEX_COMMENTS, "", sql)


def check_pivot(sql: str) -> int:
    status_code = 0

    sql_clean = replace_comments(sql)

    if sql_clean.lower().find('PIVOT') != -1:
        status_code = 1

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)

    args = parser.parse_args(argv)
    status_code = 0

    for filename in args.filenames:
        sql = Path(filename).read_text()
        status_code_file = check_pivot(
            sql
        )

        if status_code_file:
            print(
                f"{red(filename)}: contains the PIVOT() function. "
                f"Datafold does not support that."
            )

            status_code = status_code_file

    return status_code


if __name__ == "__main__":
    exit(main())