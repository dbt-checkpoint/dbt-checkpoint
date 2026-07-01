import argparse
import os
import time
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_missing_file_paths,
    get_model_sqls,
    get_models,
)


def check_group(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    exclude_pattern: str,
    groups: Optional[Sequence[str]] = None,
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
        model_group = model.node.get("config", {}).get("group") or model.node.get(
            "group"
        )

        if not model_group:
            status_code = 1
            print(
                f"{model.node.get('original_file_path', model.filename)}: "
                f"does not have a group defined.",
            )
        elif groups and model_group not in groups:
            status_code = 1
            print(
                f"{model.node.get('original_file_path', model.filename)}: "
                f"has group '{model_group}' which is not in allowed groups: {groups}.",
            )

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--groups",
        nargs="+",
        required=False,
        default=None,
        help="Optional list of allowed group names.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code = check_group(
        paths=args.filenames,
        manifest=manifest,
        exclude_pattern=args.exclude,
        groups=args.groups,
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
            "description": "Check model has group",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
