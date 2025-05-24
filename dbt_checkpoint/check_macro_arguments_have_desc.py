import argparse
import os
import time
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence
from typing import Set

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import add_default_args
from dbt_checkpoint.utils import get_config_file
from dbt_checkpoint.utils import get_dbt_manifest
from dbt_checkpoint.utils import get_filenames
from dbt_checkpoint.utils import get_macro_args_from_sql_code
from dbt_checkpoint.utils import get_macro_key
from dbt_checkpoint.utils import get_macro_schemas
from dbt_checkpoint.utils import get_macro_sqls
from dbt_checkpoint.utils import get_macros
from dbt_checkpoint.utils import get_missing_file_paths
from dbt_checkpoint.utils import JsonOpenError
from dbt_checkpoint.utils import red
from dbt_checkpoint.utils import yellow


def check_argument_desc(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    exclude_pattern: str,
    test_paths: Iterable[Path],
    config_project_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """Check if all macro arguments are documented with descriptions
     and all the documented arguments correspond to existing macro arguments.

    :param paths: filepaths to check
    :param manifest: data parsed from manifest.json
    :param exclude_pattern: file extension to exclude
    :param test_paths: test paths configured in the dbt_project.yml
    :param config_project_dir: dbt project directory path
     relative to the root of repository
    :return: status code and details of missing and/or unexpected items
    """
    status_code = 0
    missing_file_paths = get_missing_file_paths(
        paths, manifest, exclude_pattern=exclude_pattern
    )
    ymls = get_filenames(missing_file_paths, [".yml", ".yaml"])
    sqls = get_macro_sqls(missing_file_paths, manifest, config_project_dir)
    filenames = {
        get_macro_key(key, path, test_paths): path for key, path in sqls.items()
    }
    # get manifest macros that pre-commit found as changed
    macros = get_macros(manifest, filenames.keys())
    expected_documented_args: Dict[str, Set[str]] = {}
    documented_args_in_manifest: Dict[str, Set[str]] = {}
    for macro in macros:
        expected_documented_args[macro.filename] = get_macro_args_from_sql_code(macro)
        documented_args_in_manifest[macro.filename] = {
            arg.get("name")
            for arg in macro.macro.get("arguments", [])
            if arg.get("description")
        }
    # if user added schema but did not rerun the macro
    schemas = get_macro_schemas(ymls.values(), set(filenames.keys()))
    documented_args_in_schema_ymls: Dict[str, Set[str]] = {
        schema.macro_name: {
            arg.get("name")
            for arg in schema.schema.get("arguments", [])
            if arg.get("description")
        }
        for schema in schemas
    }
    all_documented_args: Dict[str, Set[str]] = {
        macro_name: documented_args_in_manifest.get(macro_name, set()).union(
            documented_args_in_schema_ymls.get(macro_name, set())
        )
        for macro_name in set(documented_args_in_schema_ymls.keys()).union(
            documented_args_in_manifest.keys()
        )
    }
    missing: Dict[str, Set[str]] = {
        macro_name: expected_args.difference(all_documented_args.get(macro_name, set()))
        for macro_name, expected_args in expected_documented_args.items()
    }
    unexpected: Dict[str, Set[str]] = {
        macro_name: documented_args.difference(
            expected_documented_args.get(macro_name, set())
        )
        for macro_name, documented_args in all_documented_args.items()
    }

    for missing_macro, arguments in missing.items():
        if arguments:
            status_code = 1
            result = "\n- ".join(list(arguments))  # pragma: no mutate
            print(
                f"{red(filenames.get(missing_macro))}: "
                f"the following arguments are missing a description:"
                f"\n- {yellow(result)}",
            )
    for unexpected_macro, arguments in unexpected.items():
        if arguments:
            status_code = 1
            result = "\n- ".join(list(arguments))  # pragma: no mutate
            print(
                f"{red(filenames.get(unexpected_macro))}: "
                f"the following arguments are documented but do"
                f" not exist in the macro implementation:\n- {yellow(result)}",
            )
    return {
        "status_code": status_code,
        "missing": missing,
        "unexpected": unexpected,
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    args = parser.parse_args(argv)

    dbt_checkpoint_config = get_config_file(args.config)
    config_project_dir = dbt_checkpoint_config.get("dbt-project-dir", "")
    dbt_project_config = get_config_file(
        Path(config_project_dir, "dbt_project.yml").as_posix()
    )
    test_paths = [
        Path(path) for path in dbt_project_config.get("test-paths", ["tests"])
    ]

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = check_argument_desc(
        paths=args.filenames,
        manifest=manifest,
        exclude_pattern=args.exclude,
        test_paths=test_paths,
        config_project_dir=config_project_dir,
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
            "description": "Check the macro arguments have description.",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
