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
    # Get all schemas from yml files (not filtered by filenames)
    # We'll filter later to only report errors for models in filenames
    all_schemas = list(get_model_schemas(list(ymls.values()), filenames, all_schemas=True))
    
    # Create a mapping of model_name -> schema for quick lookup
    schema_by_model: Dict[str, Any] = {}
    for schema in all_schemas:
        if schema.model_name in filenames:
            schema_by_model[schema.model_name] = schema
    
    # Track invalid models with their reason (missing key vs invalid value)
    invalid_models: Dict[str, Dict[str, Any]] = {}
    
    # Check each model we're validating
    for model_name in filenames:
        # Check yaml schema first (most up-to-date)
        if model_name in schema_by_model:
            schema = schema_by_model[model_name]
            meta = schema.schema.get("meta", {})
            if meta_key not in meta:
                invalid_models[model_name] = {"reason": "missing", "meta": meta, "source": "yaml"}
            elif meta.get(meta_key) not in accepted_set:
                invalid_models[model_name] = {
                    "reason": "invalid_value",
                    "meta": meta,
                    "value": meta.get(meta_key),
                    "source": "yaml",
                }
            # If valid, don't add to invalid_models
        else:
            # No yaml schema found, check manifest
            model_found = False
            for model in models:
                if model.filename == model_name:
                    model_found = True
                    meta = model.node.get("meta", {})
                    if meta_key not in meta:
                        invalid_models[model_name] = {"reason": "missing", "meta": meta, "source": "manifest"}
                    elif meta.get(meta_key) not in accepted_set:
                        invalid_models[model_name] = {
                            "reason": "invalid_value",
                            "meta": meta,
                            "value": meta.get(meta_key),
                            "source": "manifest",
                        }
                    break
            
            # If model not found in manifest either, it's missing
            if not model_found:
                invalid_models[model_name] = {"reason": "missing", "meta": {}, "source": "none"}
    
    # Find models that are invalid (missing key or invalid value)
    invalid = filenames.intersection(invalid_models.keys())
    
    for model in invalid:
        status_code = 1
        model_info = invalid_models[model]
        reason = model_info["reason"]
        
        if reason == "missing":
            print(
                f"{red(sqls.get(model))}: "
                f"meta key '{meta_key}' is missing. Accepted values: {', '.join(accepted_values)}",
            )
        else:  # invalid_value
            actual_value = model_info["value"]
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

