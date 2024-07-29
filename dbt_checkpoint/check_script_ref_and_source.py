import argparse
import os
import re
import time
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_filenames,
    red,
    replace_comments,
)


def check_refs_sources(
    paths: Sequence[str], manifest: Dict[str, Any]
) -> Dict[str, Any]:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    models = {}
    sources = {}
    version_pattern = r"version=(\d+)"
    project_name = manifest.get("metadata", {}).get("project_name")
    for _, file in sqls.items():
        full_script = file.read_text(encoding="utf-8")
        sql_clean = replace_comments(full_script)
        src_refs = re.findall(r"\{\{\s*(source|ref)\s*\((.*)\)\s*\}\}", sql_clean)
        for src_ref in src_refs:
            src_ref_value = src_ref[1].replace("'", "").replace('"', "").strip()
            if src_ref[0] == "ref":
                model_split = src_ref_value.split(",")
                version = re.search(version_pattern, src_ref_value)
                if version:
                    version = version.group(1)
                    model_split.pop()
                if len(model_split) < 2:
                    model_split.insert(0, project_name)
                package_name = model_split[0].strip()
                model_name = model_split[1].strip()
                model_version = version
                ref_key = frozenset([model_split[0].strip(), model_split[1].strip()])
                models[ref_key] = {
                    "package_name": package_name,
                    "model_name": model_name,
                    "model_version": model_version,
                }
            if src_ref[0] == "source":
                src_split = src_ref_value.split(",")
                source_name = src_split[0].strip()
                table_name = src_split[1].strip()
                src_key = frozenset([source_name, table_name])
                sources[src_key] = {
                    "source_name": source_name,
                    "table_name": table_name,
                }
    if models:
        nodes = manifest.get("nodes", {})
        for _, value in nodes.items():
            model_set = frozenset([value.get("package_name"), value.get("name")])
            if model_set in models.keys():
                model_version = models[model_set].get("model_version")
                if not model_version or (
                    model_version and not model_version != str(value.get("version"))
                ):
                    models.pop(model_set)

    if sources:
        srcs = manifest.get("sources", {})
        for _, value in srcs.items():
            source_set = frozenset([value.get("source_name"), value.get("name")])
            if source_set in sources.keys():
                sources.pop(source_set)

    for _, src in sources.items():
        status_code = 1
        source_name = src.get("source_name")  # pragma: no mutate
        table_name = src.get("table_name")  # pragma: no mutate
        print(f"Missing source `{red(f'{source_name}.{table_name}')}`")

    for _, ref in models.items():
        status_code = 1
        package_name = ref.get("package_name")  # pragma: no mutate
        model_name = ref.get("model_name")  # pragma: no mutate
        version = ref.get("model_version")  # pragma: no mutate
        msg = f"Missing model (ref) {red(f'{package_name}.{model_name}')}"
        if version:
            msg += f" with version {red(version)}"
        print(msg)

    hook_properties = {"status_code": status_code, "models": models, "sources": sources}

    return hook_properties


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args, clean=False)
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
