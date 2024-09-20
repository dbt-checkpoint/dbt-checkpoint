import argparse
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.utils import (
    JsonOpenError,
    add_catalog_args,
    add_default_args,
    get_dbt_catalog,
    get_dbt_manifest,
    red,
    strings_differ_in_case,
)


def _find_inconsistent_objects(
    manifest_objects: Dict[str, Any],
    catalog_objects: Dict[str, Any],
    objects: list,
    results: set,
):
    for object in objects:
        result = {}
        if strings_differ_in_case(
            manifest_objects[object].get("database", ""),
            catalog_objects[object].get("metadata", {}).get("database", ""),
        ):
            result["manifest"] = manifest_objects[object].get("database")
            result["catalog"] = catalog_objects[object].get("metadata").get("database")

        if strings_differ_in_case(
            manifest_objects[object].get("schema", ""),
            catalog_objects[object].get("metadata", {}).get("schema", ""),
        ):
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
    manifest: Dict[str, Any], catalog: Dict[str, Any]
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
    add_default_args(parser)
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
    status_code = check_database_casing_consistency(manifest, catalog)
    return status_code  # type: ignore


if __name__ == "__main__":
    exit(main())
