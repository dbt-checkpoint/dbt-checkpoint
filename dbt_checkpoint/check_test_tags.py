import argparse
import os
import time
from itertools import groupby
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    ParseDict,
    Test,
    add_default_args,
    get_dbt_manifest,
    get_missing_file_paths,
    get_model_sqls,
    get_models,
    get_parent_childs,
)


def validate_tags(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    tags: Sequence[str],
    exclude_pattern: str,
    include_disabled: bool = False,
) -> int:
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql", ".yml", ".yaml"], exclude_pattern=exclude_pattern
    )
    valid_tags = set(tags)
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

        for test in (test for test in childs if isinstance(test, Test)):
            test_tags = set(test.node.get("tags", []))
            
            if not test_tags.issubset(valid_tags) or not test_tags:
                status_code = 1
                list_diff = list(test_tags.difference(valid_tags))
                print(
                    f"{test.test_id} has wrong tags: {list_diff}"
                )
                
    return status_code



def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--tags",
        nargs="+",
        required=True,
        help="A list of tags that models can have." 
            " The list of tags has to be provided as space separated values."
            " eg. --tags foo bar"

    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()

    status_code = validate_tags(
        paths=args.filenames,
        manifest=manifest,
        tags=args.tags,
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
            "description": "Check model has tests by name",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
