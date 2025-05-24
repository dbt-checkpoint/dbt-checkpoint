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


def has_labels_key(
    paths: Sequence[str], labels_keys: Sequence[str], include_disabled: bool = False
) -> Dict[str, Any]:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls, include_disabled=include_disabled)

    for schema in schemas:
        schema_labels = set(schema.source_schema.get("labels", {}).keys())
        table_labels = set(schema.table_schema.get("labels", {}).keys())
        all_labels = schema_labels.union(table_labels)
        diff_labels = set(labels_keys).difference(all_labels)

        if diff_labels:
            status_code = 1
            result = "\n- ".join(list(diff_labels))
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"does not have some of the labels keys defined:\n- {result}",
            )
    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--labels-keys",
        nargs="+",
        required=True,
        help="List of required key in labels part of source.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = has_labels_key(
        paths=args.filenames,
        labels_keys=args.labels_keys,
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
            "description": "Check the source has keys in the labels part.",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
