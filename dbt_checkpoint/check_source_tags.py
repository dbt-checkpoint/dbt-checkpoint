import argparse
import os
import time
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import add_default_args
from dbt_checkpoint.utils import get_dbt_manifest
from dbt_checkpoint.utils import get_source_schemas
from dbt_checkpoint.utils import JsonOpenError


def validate_tags(
    paths: Sequence[str], tags: Sequence[str], include_disabled: bool = False
) -> Dict[str, Any]:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls, include_disabled=include_disabled)

    for schema in schemas:
        schema_tags = set(schema.source_schema.get("tags", []))
        table_tags = set(schema.table_schema.get("tags", []))
        source_tags = schema_tags.union(table_tags)
        valid_tags = set(tags)
        if not source_tags.issubset(valid_tags):
            status_code = 1
            list_diff = list(source_tags.difference(valid_tags))
            result = "\n- ".join(list_diff)  # pragma: no mutate
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"has invalid tags:\n- {result}",
            )
    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--tags",
        nargs="+",
        required=True,
        help="A list of tags that source can have.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = validate_tags(
        paths=args.filenames, tags=args.tags, include_disabled=args.include_disabled
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    status_code = hook_properties["status_code"]
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the source has valid tags.",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
