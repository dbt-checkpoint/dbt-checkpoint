import argparse
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from yaml import dump, safe_load

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    Model,
    add_catalog_args,
    add_default_args,
    get_dbt_catalog,
    get_dbt_manifest,
    get_filenames,
    get_models,
)


def append_to_properties_file(path: Path, model_schema: Dict[str, Any]) -> None:
    file = safe_load(path.open())
    if file.get("models"):
        model = file.get("models")
    else:
        model = []
        file["models"] = model
    model.append(model_schema)
    model_name = model_schema.get("name")  # pragma: no mutate
    with open(path, "w") as f:
        dump(file, f, default_flow_style=False, sort_keys=False)
        print(
            f"{path}: the schema of the `{model_name}` model was appended to the file."
        )


def write_to_properties_file(path: Path, model_schema: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file = {"version": 2, "models": [model_schema]}
    model_name = model_schema.get("name")  # pragma: no mutate
    with open(path, "w") as f:
        dump(file, f, default_flow_style=False, sort_keys=False)
        print(
            f"{path}: the schema of the `{model_name}` model was written to the file."
        )


def write_model_properties(
    path: str, model: Dict[str, Any], path_template: Dict[str, str]
) -> None:
    path_form = path.format(**path_template)
    model_path = Path(path_form)
    # It is a file and it exists
    if model_path.exists():
        append_to_properties_file(model_path, model)
    else:
        write_to_properties_file(model_path, model)


def get_model_properties(model: Model, catalog_nodes: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not model.node.get("patch_path"):
        result["name"] = model.model_name
        catalog_node = catalog_nodes.get(model.model_id, {})
        if catalog_node:
            catalog_cols = [
                {"name": col.lower()}
                for col in catalog_node.get("columns", {}).keys()
                if col
            ]
            result["columns"] = catalog_cols
        else:
            print(
                f"Unable to find model `{model.model_id}` in catalog file. "
                f"Model properties will be created without columns."
            )
    return result


def generate_properties_file(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    catalog: Dict[str, Any],
    properties_file: str,
    include_disabled: bool = False,
) -> Dict[str, Any]:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames, include_disabled=include_disabled)
    catalog_nodes = catalog.get("nodes", {})

    for model in models:
        model_prop = get_model_properties(model, catalog_nodes)
        template = {
            "database": model.node.get("database"),
            "schema": model.node.get("schema"),
            "alias": model.node.get("alias"),
            "name": model.node.get("name"),
        }
        path_template = {k: v for k, v in template.items() if v}
        if model_prop:
            status_code = 1
            write_model_properties(properties_file, model_prop, path_template)
    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    parser = argparse.ArgumentParser()
    add_default_args(parser)
    add_catalog_args(parser)

    parser.add_argument(
        "--properties-file",
        required=True,
        type=str,
        help="""Location of file where new model properties should
        be generated. Suffix has to be `yml` or `yaml`. It can also include
        {database}, {schema}, {name} and {alias} variables.
        E.g. /models/{schema}/{name}.yml for model `foo.bar` will create
        properties file in /models/foo/bar.yml.
        """,
    )

    args = parser.parse_args(argv)

    if not Path(args.properties_file).suffix in [".yml", ".yaml"]:
        print(
            "Input parameter `--schema-file` has to"
            " contain `.yml` or `.yaml` extension."
        )
        return 1

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

    start_time = time.time()
    hook_properties = generate_properties_file(
        paths=args.filenames,
        manifest=manifest,
        catalog=catalog,
        properties_file=args.properties_file,
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
            "description": "Generate model properties file.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
