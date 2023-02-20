import argparse
import os
import time
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from pre_commit_dbt.tracking import dbtCheckpointTracking
from pre_commit_dbt.utils import add_default_args
from pre_commit_dbt.utils import get_filenames
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_macro_schemas
from pre_commit_dbt.utils import get_macro_sqls
from pre_commit_dbt.utils import get_macros
from pre_commit_dbt.utils import JsonOpenError
from pre_commit_dbt.utils import red


def has_description(paths: Sequence[str], manifest: Dict[str, Any]) -> Dict[str, Any]:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_macro_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest macros that pre-commit found as changed
    macros = get_macros(manifest, filenames)
    # if user added schema but did not rerun the macro
    schemas = get_macro_schemas(list(ymls.values()), filenames)
    # convert to sets
    in_macros = {macro.filename for macro in macros if macro.macro.get("description")}
    in_schemas = {
        schema.macro_name for schema in schemas if schema.schema.get("description")
    }
    missing = filenames.difference(in_macros, in_schemas)

    for macro in missing:
        status_code = 1
        print(
            f"{red(sqls.get(macro))}: "
            f"does not have defined description or properties file is missing.",
        )
    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = has_description(paths=args.filenames, manifest=manifest)
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the macro has description.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
