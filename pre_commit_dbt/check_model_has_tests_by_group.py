import argparse
from itertools import groupby
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_model_sqls
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import get_parent_childs
from pre_commit_dbt.utils import JsonOpenError
from pre_commit_dbt.utils import Test


def check_test_cnt(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    test_group: Dict[str, int],
    test_cnt: int,
) -> int:
    status_code = 0
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)

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
    add_filenames_args(parser)
    add_manifest_args(parser)
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
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    return check_test_cnt(
        paths=args.filenames,
        manifest=manifest,
        test_group=args.tests,
        test_cnt=args.test_cnt,
    )


if __name__ == "__main__":
    exit(main())
