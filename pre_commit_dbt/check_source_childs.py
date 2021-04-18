import argparse
import operator
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


def check_child_parent_cnt(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    required_cnt: Sequence[Dict[str, Any]],
) -> int:
    status_code = 0
    ymls = [Path(path) for path in paths]

    schemas = get_source_schemas(ymls)

    for schema in schemas:
        childs = list(
            get_parent_childs(
                manifest=manifest,
                obj=schema,
                manifest_node="child_map",
                node_types=["model"],
            )
        )
        real_value = len(childs)
        for required in required_cnt:
            req_cnt = required.get("cnt")
            req_operator = required.get("operator", operator.lt)
            req_type = required.get("type")  # pragma: no mutate
            if req_cnt and req_operator(real_value, req_cnt):
                status_code = 1
                print(
                    f"{schema.source_name}.{schema.table_name}: "
                    f"has {real_value} {req_type}, but {req_type} {req_cnt}"
                    f"is/are required.",
                )

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)

    parser.add_argument(
        "--min-child-cnt",
        type=int,
        default=0,
        help="Minimal number of child relations.",
    )

    parser.add_argument(
        "--max-child-cnt",
        type=int,
        help="Miximal number of child relations.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    required_cnt = [
        {
            "operator": operator.lt,
            "type": "min",
            "dep": "childs",
            "cnt": args.min_child_cnt,
        },
        {
            "operator": operator.gt,
            "type": "max",
            "dep": "childs",
            "cnt": args.max_child_cnt,
        },
    ]
    return check_child_parent_cnt(
        paths=args.filenames, manifest=manifest, required_cnt=required_cnt
    )


if __name__ == "__main__":
    exit(main())
