import argparse
import os
import time
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_filenames,
    get_json,
    get_missing_file_paths,
    get_models,
    get_parent_childs,
)


def check_parents_schema(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    blacklist: Optional[Sequence[str]],
    whitelist: Optional[Sequence[str]],
    schema_location: Optional[str],
) -> int:
    paths = get_missing_file_paths(paths, manifest)

    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    blacklist = blacklist or []
    whitelist = whitelist or []
    schema_location = schema_location or []

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)

    for model in models:
        parents = list(
            get_parent_childs(
                manifest=manifest,
                obj=model,
                manifest_node="parent_map",
                node_types=["model", "source"],
            )
        )
        for parent in parents:
            # Selecting the location of the model schema
            if schema_location == "config": # pragma: no cover
                # Chooses the schema inside in the model config (nodes -> model -> config -> schema)
                db = parent.node.get("config").get("schema")
            else: # pragma: no cover
                # Chooses the schema outside the model config (nodes -> model -> schema)
                db = parent.node.get("schema")
            if (whitelist and db not in whitelist) or db in blacklist:
                status_code = 1
                print(
                    f"{model.model_name}: "
                    f"has parent {parent.node.get('name')} with invalid schema "
                    f"{db}.",
                )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    white_black = parser.add_mutually_exclusive_group()

    white_black.add_argument(
        "--whitelist",
        nargs="+",
        help="Whitelisted schemas.",
    )

    white_black.add_argument(
        "--blacklist",
        nargs="+",
        help="Blacklisted schemas.",
    )

    parser.add_argument(
        "--schema_location",
        type=str,
        nargs="?",
        required=False,
        default="",
        help="Location of the model schema.",
    ),

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    if not (args.blacklist or args.whitelist):
        print("Please specify at least one `--blacklist` or `--whitelist` option.")
        return 1

    start_time = time.time()
    status_code = check_parents_schema(
        paths=args.filenames,
        manifest=manifest,
        blacklist=args.blacklist,
        whitelist=args.whitelist,
        schema_location=args.schema_location,
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check model parents schema",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())