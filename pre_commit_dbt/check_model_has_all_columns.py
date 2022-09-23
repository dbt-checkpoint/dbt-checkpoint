import argparse
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

from pre_commit_dbt.utils import add_catalog_args
from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_missing_file_paths
from pre_commit_dbt.utils import get_model_sqls
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import JsonOpenError
from pre_commit_dbt.utils import red
from pre_commit_dbt.utils import yellow


def compare_columns(
    catalog_columns: Dict[str, Any], model_columns: Dict[str, Any]
) -> Tuple[Set[str], Set[str]]:
    catalog_cols = {col.lower() for col in catalog_columns.keys()}
    model_cols = {col.lower() for col in model_columns.keys()}
    model_only = model_cols.difference(catalog_cols)
    catalog_only = catalog_cols.difference(model_cols)
    return model_only, catalog_only


def check_model_columns(
    paths: Sequence[str], manifest: Dict[str, Any], catalog: Dict[str, Any]
) -> int:
    paths = get_missing_file_paths(paths, manifest)

    status_code = 0
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)

    catalog_nodes = catalog.get("nodes", {})

    for model in models:
        catalog_node = catalog_nodes.get(model.model_id, {})
        if catalog_node:
            model_only, catalog_only = compare_columns(
                catalog_columns=catalog_node.get("columns", {}),
                model_columns=model.node.get("columns", {}),
            )
            schema_path = model.node.get("patch_path", "schema")  # pragma: no mutate
            if not schema_path:
                schema_path = "any .yml file"  # pragma: no cover
            if model_only:
                status_code = 1
                print_cols = ["- name: %s" % yellow(col) for col in model_only if col]
                print(
                    "Columns in {schema_path}, but not in Database ({file}):\n"
                    "{columns}".format(
                        file=red(sqls.get(model.filename)),
                        columns="\n".join(print_cols),  # pragma: no mutate
                        schema_path=yellow(schema_path),
                    )
                )
            if catalog_only:
                status_code = 1
                print_cols = ["- name: %s" % red(col) for col in catalog_only if col]
                print(
                    "Columns in Database ({file}), but not in {schema_path}:\n"
                    "{columns}".format(
                        file=red(sqls.get(model.filename)),
                        columns="\n".join(print_cols),  # pragma: no mutate
                        schema_path=yellow(schema_path),
                    )
                )
        else:
            status_code = 1
            print(
                f"Unable to find model `{red(model.model_id)}` in catalog file. "
                f"Make sure you run `dbt docs generate` before executing this hook."
            )
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

    return check_model_columns(paths=args.filenames, manifest=manifest, catalog=catalog)


if __name__ == "__main__":
    exit(main())
