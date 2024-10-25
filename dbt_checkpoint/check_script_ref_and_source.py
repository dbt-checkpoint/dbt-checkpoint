import argparse
import os
import re
import time
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.check_script_has_no_table_name import replace_comments
from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_filenames,
    get_manifest_node_from_file_path,
    red,
)


def obj_exists_in_manifest(
    obj_name: str,
    manifest_sources: Dict[str, Any],
    manifest_nodes: Dict[str, Any],
    is_source: bool = False,
) -> bool:
    if is_source:
        lookup_obj = manifest_sources
    else:
        lookup_obj = manifest_nodes
    for _, value in lookup_obj.items():
        lookup_name = value.get("unique_id")
        if obj_name == lookup_name:
            return True
    return False


def check_refs_sources(
    paths: Sequence[str], manifest: Dict[str, Any]
) -> Dict[str, Any]:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    manifest_sources = manifest.get("sources", {})
    manifest_nodes = manifest.get("nodes", {})
    models = set()
    sources = {}
    for _, file in sqls.items():
        filename = str(file)
        ref_node = get_manifest_node_from_file_path(manifest, str(file))
        dependent_nodes = ref_node.get("depends_on", {}).get("nodes", [])
        for node in dependent_nodes:
            node_split = node.split(".")
            node_type = node_split[0]
            dependency_exists = obj_exists_in_manifest(
                node,
                manifest_sources,
                manifest_nodes,
                is_source=node_type == "source",
            )
            if not dependency_exists:
                status_code = 1
                print(f"Missing {node_type} {red(node)} in {red(filename)}")
                if node_type == "model":
                    models.add(node)
                if node_type == "source":
                    source_name = node_split[-2]
                    table_name = node_split[-1]
                    src_key = frozenset([source_name, table_name])
                    sources[src_key] = {
                        "source_name": source_name,
                        "table_name": table_name,
                    }

    hook_properties = {"status_code": status_code, "models": models, "sources": sources}
    return hook_properties


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    script_args = vars(args)

    start_time = time.time()
    hook_properties = check_refs_sources(paths=args.filenames, manifest=manifest)
    end_time = time.time()

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": " Check the script has only existing refs and sources.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
