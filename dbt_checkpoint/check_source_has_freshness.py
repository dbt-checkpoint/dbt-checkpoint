import argparse
import os
import time
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Set

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import add_default_args
from dbt_checkpoint.utils import get_dbt_manifest
from dbt_checkpoint.utils import get_source_schemas
from dbt_checkpoint.utils import JsonOpenError
from dbt_checkpoint.utils import red
from dbt_checkpoint.utils import yellow


def has_freshness(
    paths: Sequence[str], required_freshness: Set[str], include_disabled: bool = False
) -> Dict[str, Any]:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls, include_disabled=include_disabled)

    for schema in schemas:
        source = schema.source_schema
        table = schema.table_schema
        merged = {**source.get("freshness", {}), **table.get("freshness", {})}
        # if filter is present, remove it because it's not a dictionary like
        # warn_after or error_after
        if "filter" in merged.keys():
            merged.pop("filter")
        freshness = {
            key
            for key, value in merged.items()
            if set(value.keys()) == {"count", "period"}
        }
        loaded = table.get("loaded_at_field") or source.get("loaded_at_field")
        if not loaded:
            status_code = 1
            print(
                f"{red(f'{schema.source_name}.{schema.table_name}')}: "
                f"is missing `loaded_at_field` which is required for freshness."
            )
        if not (freshness == required_freshness):
            status_code = 1
            missing_params = required_freshness.difference(freshness)
            result = "\n- ".join(list(missing_params))  # pragma: no mutate
            print(
                f"{red(f'{schema.source_name}.{schema.table_name}')}: "
                f"miss some required freshness parameters:"
                f"\n- {yellow(result)} "
            )
    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--freshness",
        nargs="+",
        required=True,
        choices=["warn_after", "error_after"],
        help="List of required freshness options.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = has_freshness(
        paths=args.filenames,
        required_freshness=set(args.freshness),
        include_disabled=args.include_disabled,
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
            "description": "Check the source has the freshness.",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
