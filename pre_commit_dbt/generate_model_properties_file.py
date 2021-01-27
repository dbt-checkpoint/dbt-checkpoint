import argparse
from pathlib import Path
from typing import Any
from typing import Dict
from typing import NoReturn
from typing import Optional
from typing import Sequence

import yaml

from pre_commit_dbt.utils import add_catalog_args
from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_filenames
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import JsonOpenError
from pre_commit_dbt.utils import Model


def append_to_properties_file(path: Path, model_schema: Dict[str, Any]) -> NoReturn:
    file = yaml.safe_load(path.open())
    if file.get("models"):
        model = file.get("models")
    else:
        model = []
        file["models"] = model
    model.append(model_schema)
    model_name = model_schema.get("name")  # pragma: no mutate
    with open(path, "w") as f:
        yaml.dump(file, f, default_flow_style=False, sort_keys=False)
        print(
            f"{path}: the schema of the `{model_name}` model was appended to the file."
        )


def write_to_properties_file(path: Path, model_schema: Dict[str, Any]) -> NoReturn:
    path.parent.mkdir(parents=True, exist_ok=True)
    file = {"version": 2, "models": [model_schema]}
    model_name = model_schema.get("name")  # pragma: no mutate
    with open(path, "w") as f:
        yaml.dump(file, f, default_flow_style=False, sort_keys=False)
        print(
            f"{path}: the schema of the `{model_name}` model was written to the file."
        )


def write_model_properties(
    path: str, model: Dict[str, Any], path_template: Dict[str, str]
) -> NoReturn:
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
) -> int:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)
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
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)
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
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    try:
        catalog = get_json(args.catalog)
    except JsonOpenError as e:
        print(f"Unable to load catalog file ({e})")
        return 1

    status_code = generate_properties_file(
        paths=args.filenames,
        manifest=manifest,
        catalog=catalog,
        properties_file=args.properties_file,
    )
    return status_code


if __name__ == "__main__":
    exit(main())
