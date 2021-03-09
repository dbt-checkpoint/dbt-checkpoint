import argparse
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_parent_childs
from pre_commit_dbt.utils import get_source_schemas
from pre_commit_dbt.utils import JsonOpenError


def check_test_cnt(
    paths: Sequence[str], manifest: Dict[str, Any], test_cnt: int
) -> int:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        tests = list(
            get_parent_childs(
                manifest=manifest,
                obj=schema,
                manifest_node="child_map",
                node_types=["test"],
            )
        )
        source_test_cnt = len(tests)
        if source_test_cnt < test_cnt:
            status_code = 1
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"has only {source_test_cnt} tests, but {test_cnt} are required.",
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)

    parser.add_argument(
        "--test-cnt",
        type=int,
        default=1,
        help="Minimum number of tests required.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    return check_test_cnt(
        paths=args.filenames, manifest=manifest, test_cnt=args.test_cnt
    )


if __name__ == "__main__":
    exit(main())
