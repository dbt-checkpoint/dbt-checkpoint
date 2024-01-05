import argparse
import os
import time
from itertools import groupby
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    Test,
    add_default_args,
    get_dbt_manifest,
    get_missing_file_paths,
    get_model_sqls,
    get_models,
    get_parent_childs,
)


def check_test_cnt(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    test_group: Dict[str, int],
    test_cnt: int,
    exclude_pattern: str,
    include_disabled: bool = False,
) -> int:
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
    )

    status_code = 0
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames, include_disabled=include_disabled)

    for model in models:
        childs = list(
            get_parent_childs(
                manifest=manifest,
                obj=model,
                manifest_node="child_map",
                node_types=["test"],
            )
        )
        tests = [test for test in childs if isinstance(test, Test)]
        grouped = groupby(
            sorted(tests, key=lambda x: x.test_name), lambda x: x.test_name
        )
        test_dict = {key: list(value) for key, value in grouped}
        required_test_count = 0
        for test in test_group:
            if test_dict.get(test):
                required_test_count += 1
        if required_test_count < test_cnt:
            print(
                f"{model.model_name}: "
                f"has only {required_test_count} test(s) from {test_group}.",
            )
            status_code = 1
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    parser.add_argument(
        "--tests",
        nargs="+",
        required=True,
        help="List of acceptable tests.",
    )
    parser.add_argument(
        "--test-cnt",
        type=int,
        default=1,
        help="Minimum number of tests required.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code = check_test_cnt(
        paths=args.filenames,
        manifest=manifest,
        test_group=args.tests,
        test_cnt=args.test_cnt,
        exclude_pattern=args.exclude,
        include_disabled=args.include_disabled,
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check model has tests by group",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
