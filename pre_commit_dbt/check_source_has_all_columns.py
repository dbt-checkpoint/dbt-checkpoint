import argparse
from pathlib import Path
from typing import Any
from typing import Dict
from typing import FrozenSet
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

from pre_commit_dbt.utils import add_catalog_args
from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_source_schemas
from pre_commit_dbt.utils import JsonOpenError


def compare_source_columns(
    catalog_columns: Dict[str, Any], schema_columns: Sequence[Dict[str, Any]]
) -> Tuple[Set[str], Set[str]]:
    catalog_cols = {col.lower() for col in catalog_columns.keys()}
    schema_cols = {str(col.get("name")) for col in schema_columns if col.get("name")}
    schema_only = schema_cols.difference(catalog_cols)
    catalog_only = catalog_cols.difference(schema_cols)
    return schema_only, catalog_only


def get_catalog_nodes(catalog: Dict[str, Any]) -> Dict[FrozenSet[str], Any]:
    catalog_nodes = {}
    for node_id, node in catalog.get("sources", {}).items():
        key_split = node_id.split(".")
        source_name = key_split[-2]
        table_name = key_split[-1]
        catalog_nodes[frozenset([source_name, table_name])] = node
    return catalog_nodes


def check_source_columns(paths: Sequence[str], catalog: Dict[str, Any]) -> int:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    catalog_nodes = get_catalog_nodes(catalog)

    for schema in schemas:
        schema_id = frozenset([schema.source_name, schema.table_name])
        catalog_node = catalog_nodes.get(schema_id, {})
        if catalog_node:
            schema_only, catalog_only = compare_source_columns(
                catalog_columns=catalog_node.get("columns", {}),
                schema_columns=schema.table_schema.get("columns", []),
            )
            if schema_only:
                status_code = 1
                print_cols = [
                    f"- name: {col}" for col in schema_only if col
                ]  # pragma: no mutate
                print(
                    "{file}: columns in {schema_path} but not in db (catalog.json):\n"
                    "{columns}".format(
                        file=schema.source_name
                        + "."  # pragma: no mutate
                        + schema.table_name,
                        columns="\n".join(print_cols),  # pragma: no mutate
                        schema_path=schema.filename,
                    )
                )
            if catalog_only:
                status_code = 1
                print_cols = [f"- name: {col}" for col in catalog_only if col]
                print(
                    "{file}: columns in db (catalog.json) but not in {schema_path}:\n"
                    "{columns}".format(
                        file=schema.source_name
                        + "."  # pragma: no mutate
                        + schema.table_name,
                        columns="\n".join(print_cols),  # pragma: no mutate
                        schema_path=schema.filename,
                    )
                )
        else:
            status_code = 1
            print(
                f"Unable to find source `{schema.source_name}.{schema.table_name}` "
                f"in catalog file. Make sure you run `dbt docs generate` before "
                f"executing this hook."
            )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_catalog_args(parser)

    args = parser.parse_args(argv)

    try:
        catalog = get_json(args.catalog)
    except JsonOpenError as e:
        print(f"Unable to load catalog file ({e})")
        return 1

    return check_source_columns(paths=args.filenames, catalog=catalog)


if __name__ == "__main__":
    exit(main())
