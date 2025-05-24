import argparse
import os
import time
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import add_default_args
from dbt_checkpoint.utils import get_dbt_manifest
from dbt_checkpoint.utils import get_filenames
from dbt_checkpoint.utils import get_missing_file_paths
from dbt_checkpoint.utils import get_model_schemas
from dbt_checkpoint.utils import get_model_sqls
from dbt_checkpoint.utils import get_models
from dbt_checkpoint.utils import JsonOpenError
from dbt_checkpoint.utils import red


def has_description(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    exclude_pattern: str,
    include_disabled: bool = False,
) -> Dict[str, Any]:
    paths = get_missing_file_paths(  # type: ignore
        paths, manifest, exclude_pattern=exclude_pattern
    )

    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames, include_disabled=include_disabled)

    # if user added schema but did not rerun the model
    schemas = get_model_schemas(list(ymls.values()), filenames)
    # convert to sets
    in_models = {model.filename for model in models if model.node.get("description")}
    in_schemas = {
        schema.model_name for schema in schemas if schema.schema.get("description")
    }
    missing = filenames.difference(in_models, in_schemas)

    for model in missing:
        status_code = 1
        print(
            f"{red(sqls.get(model))}: "
            f"does not have defined description or properties file is missing.",
        )
    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = has_description(
        paths=args.filenames,
        manifest=manifest,
        exclude_pattern=args.exclude,
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
            "description": "Check the model has description",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code  # type: ignore


if __name__ == "__main__":
    exit(main())
