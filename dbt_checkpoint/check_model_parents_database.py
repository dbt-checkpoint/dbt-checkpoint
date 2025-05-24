import argparse
import os
import time
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import add_default_args
from dbt_checkpoint.utils import get_dbt_manifest
from dbt_checkpoint.utils import get_filenames
from dbt_checkpoint.utils import get_missing_file_paths
from dbt_checkpoint.utils import get_models
from dbt_checkpoint.utils import get_parent_childs
from dbt_checkpoint.utils import JsonOpenError


def check_parents_database(
    paths: Iterable[str],
    manifest: Dict[str, Any],
    blacklist: Optional[Sequence[str]],
    whitelist: Optional[Sequence[str]],
    exclude_pattern: str,
    include_disabled: bool = False,
) -> int:
    paths = get_missing_file_paths(
        paths, manifest, True, exclude_pattern=exclude_pattern
    )

    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    blacklist = blacklist or []
    whitelist = whitelist or []

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames, include_disabled=include_disabled)

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
            db = parent.node.get("database")
            if (whitelist and db not in whitelist) or db in blacklist:
                status_code = 1
                print(
                    f"{model.model_name}: "
                    f"has parent {parent.node.get('name')} with invalid database "
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
        help="Whitelisted databases.",
    )

    white_black.add_argument(
        "--blacklist",
        nargs="+",
        help="Blacklisted databases.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    if not (args.blacklist or args.whitelist):
        print("Please specify at least one `--blacklist` or `--whitelist` option.")
        return 1

    start_time = time.time()
    status_code = check_parents_database(
        paths=args.filenames,
        manifest=manifest,
        blacklist=args.blacklist,
        whitelist=args.whitelist,
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
            "description": "Check model parents database",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
