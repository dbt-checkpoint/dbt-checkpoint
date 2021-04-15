import argparse
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_filenames
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import get_parent_childs
from pre_commit_dbt.utils import JsonOpenError


def check_parents_database(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    blacklist: Optional[Sequence[str]],
    whitelist: Optional[Sequence[str]],
) -> int:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())
    blacklist = blacklist or []
    whitelist = whitelist or []

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
    add_filenames_args(parser)
    add_manifest_args(parser)

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
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    if not (args.blacklist or args.whitelist):
        print("Please specify at least one `--blacklist` or `--whitelist` option.")
        return 1

    return check_parents_database(
        paths=args.filenames,
        manifest=manifest,
        blacklist=args.blacklist,
        whitelist=args.whitelist,
    )


if __name__ == "__main__":
    exit(main())
