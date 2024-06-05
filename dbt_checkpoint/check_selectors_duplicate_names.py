import argparse
import os
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence

import yaml

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import add_default_args


def check_selectors_duplicate_names(data: Dict[str, Any]) -> List[str]:
    errors = []
    selector_names = set()
    duplicates = set()

    for selector in data.get("selectors", []):
        name = selector.get("name")
        if name in selector_names:
            duplicates.add(name)
        else:
            selector_names.add(name)

    if duplicates:
        for name in duplicates:
            errors.append(f"Duplicate selector name found: '{name}'")

    return errors


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    args = parser.parse_args(argv)

    error_flag = False
    script_args = vars(args)

    start_time = time.time()
    for file_path in args.filenames:
        try:
            with open(file_path, "r") as file:
                data = yaml.safe_load(file)
                errors = check_selectors_duplicate_names(data)
                if errors:
                    print(f"Errors found in '{file_path}':")
                    for error in errors:
                        print(error)
                    error_flag = True
        except Exception as e:
            print(f"Failed to process '{file_path}': {e}")
            error_flag = True
    end_time = time.time()

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=None,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check for duplicate selector names",
            "status": 1 if error_flag else 0,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return 1 if error_flag else 0


if __name__ == "__main__":
    exit(main())
