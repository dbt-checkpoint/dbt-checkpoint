import argparse
import os
import time
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Optional
from typing import Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import add_default_args
from dbt_checkpoint.utils import get_config_file
from dbt_checkpoint.utils import get_dbt_manifest
from dbt_checkpoint.utils import get_filenames
from dbt_checkpoint.utils import get_macro_key
from dbt_checkpoint.utils import get_macro_schemas
from dbt_checkpoint.utils import get_macro_sqls
from dbt_checkpoint.utils import get_macros
from dbt_checkpoint.utils import get_missing_file_paths
from dbt_checkpoint.utils import JsonOpenError
from dbt_checkpoint.utils import red


def has_description(
    paths: Iterable[str],
    manifest: Dict[str, Any],
    exclude_pattern: str,
    test_paths: Iterable[Path],
    config_project_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """Check if all macros have descriptions.

    :param paths: filepaths to check
    :param manifest: data parsed from manifest.json
    :param exclude_pattern: file extension to exclude
    :param test_paths: test paths configured in the dbt_project.yml
    :param config_project_dir: dbt project directory path
     relative to the root of repository
    :return: status code and details of missing and/or unexpected items
    """
    missing_file_paths = get_missing_file_paths(
        paths, manifest, exclude_pattern=exclude_pattern
    )
    status_code = 0
    ymls = get_filenames(missing_file_paths, [".yml", ".yaml"])
    sqls = get_macro_sqls(missing_file_paths, manifest, config_project_dir)
    filenames = {
        get_macro_key(key, path, test_paths): path for key, path in sqls.items()
    }
    # get manifest macros that pre-commit found as changed
    macros = get_macros(manifest, set(filenames.keys()))
    # if user added schema but did not rerun the macro
    schemas = get_macro_schemas(ymls.values(), set(filenames.keys()))
    # convert to sets
    in_macros = {macro.filename for macro in macros if macro.macro.get("description")}
    in_schemas = {
        schema.macro_name for schema in schemas if schema.schema.get("description")
    }
    documented_macros = in_macros.union(in_schemas)
    missing = set(filenames.keys()).difference(documented_macros)

    for macro in missing:
        status_code = 1
        print(
            f"{red(filenames.get(macro))}: "
            f"does not have defined description or properties file is missing.",
        )
    return {"status_code": status_code}


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

    hook_properties = has_description(
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
            "description": "Check the macro has description.",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
