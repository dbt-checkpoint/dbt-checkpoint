import argparse
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_catalog_args
from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_filenames
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import JsonOpenError
from pre_commit_dbt.utils import Model


def write_model_properties():
    pass


def get_model_properties(model: Model, catalog_nodes: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    if not model.node.get("patch_path"):
        result["name"] = model.model_name
        catalog_node = catalog_nodes.get(model.model_id, {})
        if catalog_node:
            catalog_cols = [
                {"name": col.get("name")}
                for col in catalog_node.get("columns", {}).keys()
                if col.get("name")
            ]
            result["columns"] = catalog_cols
        else:
            print(
                f"Unable to find model `{model.model_id}` in catalog file. "
                f"Model properties will be created without columns."
            )
    return result


def generate_properties_file(
    paths: Sequence[str], manifest: Dict[str, Any], catalog: Dict[str, Any]
) -> int:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)
    catalog_nodes = catalog.get("nodes", {})

    for model in models:
        model_prop = get_model_properties(model, catalog_nodes)
        if model_prop:
            status_code = 1

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)
    add_catalog_args(parser)

    args = parser.parse_args(argv)

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
        paths=args.filenames, manifest=manifest, catalog=catalog
    )
    return status_code


if __name__ == "__main__":
    exit(main())
