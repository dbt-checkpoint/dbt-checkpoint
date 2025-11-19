import argparse
import os
import time
from typing import Any, Dict, Optional, Sequence, Set

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_filenames,
    get_missing_file_paths,
    get_model_schemas,
    get_model_sqls,
    get_models,
    red,
)


def check_meta_key_accepted_values(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    meta_key: str,
    accepted_values: Sequence[str],
    exclude_pattern: str = "",
    include_disabled: bool = False,
) -> int:
    # Discover related SQL files when yaml files are changed
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql", ".yml", ".yaml"], exclude_pattern=exclude_pattern
    )
    
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())
    accepted_set = set(accepted_values)
    
    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames, include_disabled=include_disabled)
    # if user added schema but did not rerun the model
    schemas = get_model_schemas(list(ymls.values()), filenames)
    
    # Check models from manifest
    invalid_models: Set[str] = set()
    for model in models:
        meta = model.node.get("meta", {})
        if meta_key not in meta:
            invalid_models.add(model.filename)
        elif meta.get(meta_key) not in accepted_set:
            invalid_models.add(model.filename)
    
    # Check schemas from yml files
    for schema in schemas:
        meta = schema.schema.get("meta", {})
        if meta_key not in meta:
            invalid_models.add(schema.model_name)
        elif meta.get(meta_key) not in accepted_set:
            invalid_models.add(schema.model_name)
    
    # Find models that are invalid (missing key or invalid value)
    invalid = filenames.intersection(invalid_models)
    
    for model in invalid:
        status_code = 1
        meta = {}
        # Try to get meta from model or schema
        for m in models:
            if m.filename == model:
                meta = m.node.get("meta", {})
                break
        if not meta:
            for s in schemas:
                if s.model_name == model:
                    meta = s.schema.get("meta", {})
                    break
        
        if meta_key not in meta:
            print(
                f"{red(sqls.get(model))}: "
                f"meta key '{meta_key}' is missing. Accepted values: {', '.join(accepted_values)}",
            )
        else:
            actual_value = meta.get(meta_key)
            print(
                f"{red(sqls.get(model))}: "
                f"meta key '{meta_key}' has value '{actual_value}' which is not in accepted values: {', '.join(accepted_values)}",
            )
    
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    parser.add_argument(
        "--meta-key",
        type=str,
        required=True,
        help="Name of the meta key to check (e.g. 'domain').",
    )
    parser.add_argument(
        "--accepted-values",
        nargs="+",
        required=True,
        help="List of accepted values for the meta key (e.g. 'sales finance hr').",
    )
    
    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    status_code = check_meta_key_accepted_values(
        paths=args.filenames,
        manifest=manifest,
        meta_key=args.meta_key,
        accepted_values=args.accepted_values,
        exclude_pattern=args.exclude,
        include_disabled=args.include_disabled,
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check model meta key has accepted values",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())

