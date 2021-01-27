import argparse
from typing import Optional
from typing import Sequence

from pre_commit_dbt.check_script_semicolon import check_semicolon
from pre_commit_dbt.utils import add_filenames_args


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)

    args = parser.parse_args(argv)
    status_code = 0

    for filename in args.filenames:
        with open(filename, "rb+") as file_obj:
            status_code_file = check_semicolon(file_obj, replace=True)
            if status_code_file:
                print(f"Replacing semicolon in {filename}.")
                status_code = status_code_file

    return status_code


if __name__ == "__main__":
    exit(main())
