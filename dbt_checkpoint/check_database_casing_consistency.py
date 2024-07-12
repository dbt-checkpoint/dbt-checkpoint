import argparse
import os
import time
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_catalog_args,
    add_config_args,
    add_manifest_args,
    add_tracking_args,
    get_dbt_catalog,
    get_dbt_manifest,
    red,
)


def _find_inconsistent_objects(
    manifest_objects: Dict[str, Any],
    catalog_objects: Dict[str, Any],
    objects: list,
    results: set,
):
    for object in objects:
        result = {}
        if manifest_objects[object].get("database") != catalog_objects[object].get(
            "metadata", {}
        ).get("database"):
            result["manifest"] = manifest_objects[object].get("database")
            result["catalog"] = catalog_objects[object].get("metadata").get("database")
        if manifest_objects[object].get("schema") != catalog_objects[object].get(
            "metadata", {}
        ).get("schema"):
            result["manifest"] = (
                f"{manifest_objects[object].get('database')}.{manifest_objects[object].get('schema')}"
            )
            result["catalog"] = (
                f"{catalog_objects[object].get('metadata').get('database')}.{catalog_objects[object].get('metadata').get('schema')}"
            )
        if result:
            result_message = f"{red(result['manifest'])} in dbt project (manifest) does not match {red(result['catalog'])} in database (catalog)"
            results.add(result_message)


def check_database_casing_consistency(
    manifest: Dict[str, Any], catalog: Dict[str, Any], adapter: str = None
):
    return_code = 0
    results = set()
    catalog_nodes = catalog.get("nodes", {})
    manifest_nodes = manifest.get("nodes", {})
    matching_nodes = [
        key for key in manifest_nodes.keys() if key in catalog_nodes.keys()
    ]
    catalog_sources = catalog.get("sources", {})
    manifest_sources = manifest.get("sources", {})
    matching_sources = [
        key for key in manifest_sources.keys() if key in catalog_sources.keys()
    ]

    _find_inconsistent_objects(manifest_nodes, catalog_nodes, matching_nodes, results)
    _find_inconsistent_objects(
        manifest_sources, catalog_sources, matching_sources, results
    )
    # If inconsistent_objects
    if len(results) > 0:
        return_code = 1
        print("Found casing inconsistencies:")
        for result in results:
            print(result)
    return return_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_manifest_args(parser)
    add_tracking_args(parser)
    add_config_args(parser)
    add_catalog_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1
    try:
        catalog = get_dbt_catalog(args)
    except JsonOpenError as e:
        print(f"Unable to load catalog file ({e})")
        return 1
    dbt_adapter = manifest.get("metadata", {}).get("dbt_adapter")
    start_time = time.time()
    status_code = check_database_casing_consistency(
        manifest=manifest, catalog=catalog, adapter=dbt_adapter
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
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
