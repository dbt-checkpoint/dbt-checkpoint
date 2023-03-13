import argparse
import os
import time
from typing import Optional
from typing import Sequence

from pre_commit_dbt.check_script_semicolon import check_semicolon
from pre_commit_dbt.tracking import dbtCheckpointTracking
from pre_commit_dbt.utils import add_default_args
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import JsonOpenError


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    args = parser.parse_args(argv)
    status_code = 0

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()

    for filename in args.filenames:
        with open(filename, "rb+") as file_obj:
            status_code_file = check_semicolon(file_obj, replace=True)
            if status_code_file:
                print(f"Replacing semicolon in {filename}.")
                status_code = status_code_file

    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Remove the semicolon at the end of the script.",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
