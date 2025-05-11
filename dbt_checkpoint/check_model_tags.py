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


def validate_tags(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    tags: Sequence[str],
    exclude_pattern: str,
    has_any_tag: bool = False,
    has_all_tags: bool = False,
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
        # tags can be specified only from manifest
        model_tags = set(model.node.get("tags", []))
        valid_tags = set(tags)

        # check if model has any tags that are not in the valid list
        if not model_tags.issubset(valid_tags):
            status_code = 1
            list_diff = list(model_tags.difference(valid_tags))
            result = "\n- ".join(list_diff)  # pragma: no mutate
            print(
                f"{model.node.get('original_file_path', model.filename)}: "
                f"has invalid tags:\n- {result}",
            )

        if has_any_tag:
            # check if model has at least one tag in the provided tags list
            if not any(valid_tag in model_tags for valid_tag in valid_tags):
                status_code = 1
                print(
                    f"{model.node.get('original_file_path', model.filename)}: "
                    f"does not have any valid tags: {valid_tags}",
                )

        if has_all_tags:
            # check if model has all tags in the provided tags list
            if not all(valid_tag in model_tags for valid_tag in valid_tags):
                status_code = 1
                print(
                    f"{model.node.get('original_file_path', model.filename)}: "
                    f"does not have all valid tags: {valid_tags}",
                )

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--tags",
        nargs="+",
        required=True,
        help="A list of tags that models can have.",
    )

    opt_args = parser.add_mutually_exclusive_group()

    opt_args.add_argument(
        "--has-any-tag",
        action="store_true",
        required=False,
        help="True/False check if the model has at least one valid tag",
    )

    opt_args.add_argument(
        "--has-all-tags",
        action="store_true",
        required=False,
        help="True/False check if the model has all expected tags",
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
            "description": "Check model tags",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
