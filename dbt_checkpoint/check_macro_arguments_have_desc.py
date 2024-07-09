import argparse
import itertools
import os
import time
from typing import Any, Dict, Optional, Sequence, Set

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    Macro,
    MacroSchema,
    add_default_args,
    get_dbt_manifest,
    get_filenames,
    get_macro_schemas,
    get_macro_sqls,
    get_macros,
    red,
    yellow,
)


def check_argument_desc(
    paths: Sequence[str], manifest: Dict[str, Any]
) -> Dict[str, Any]:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_macro_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest macros that pre-commit found as changed
    macros = get_macros(manifest, filenames)
    # if user added schema but did not rerun the macro
    schemas = get_macro_schemas(list(ymls.values()), filenames)
    missing: Dict[str, Set[str]] = {}

    for item in itertools.chain(macros, schemas):
        missing_args = set()  # pragma: no mutate
        if isinstance(item, MacroSchema):
            macro_name = item.macro_name
            missing_args = {
                key.get("name")
                for key in item.schema.get("arguments", [])
                if not key.get("description")
            }
        # Macro
        elif isinstance(item, Macro):
            macro_name = item.filename
            missing_args = {
                arg.get("name")
                for arg in item.macro.get("arguments", [])
                if not arg.get("description")
            }
        else:
            continue
        seen = missing.get(macro_name)
        if seen:
            if not missing_args:
                missing[macro_name] = set()  # pragma: no mutate
            else:
                missing[macro_name] = seen.union(missing_args)
        elif missing_args:
            missing[macro_name] = missing_args

    for macro, arguments in missing.items():
        if arguments:
            status_code = 1
            result = "\n- ".join(list(arguments))  # pragma: no mutate
            print(
                f"{red(sqls.get(macro))}: "
                f"following arguments are missing description:\n- {yellow(result)}",
            )
    return {"status_code": status_code, "missing": missing}


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
    hook_properties = check_argument_desc(paths=args.filenames, manifest=manifest)
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the macro arguments have description.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
