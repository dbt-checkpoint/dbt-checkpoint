import argparse
import os
import time
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

from dbt_checkpoint.result_logging import ResultLogger
from dbt_checkpoint.result_logging import ResultSummary
from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import add_catalog_args
from dbt_checkpoint.utils import add_default_args
from dbt_checkpoint.utils import get_dbt_catalog
from dbt_checkpoint.utils import get_dbt_manifest
from dbt_checkpoint.utils import get_missing_file_paths
from dbt_checkpoint.utils import get_model_sqls
from dbt_checkpoint.utils import get_models
from dbt_checkpoint.utils import JsonOpenError
from dbt_checkpoint.utils import red
from dbt_checkpoint.utils import yellow


def compare_columns(
    catalog_columns: Dict[str, Any], model_columns: Dict[str, Any]
) -> Tuple[Set[str], Set[str]]:
    catalog_cols = {col.lower() for col in catalog_columns.keys()}
    model_cols = {col.lower() for col in model_columns.keys()}
    model_only = model_cols.difference(catalog_cols)
    catalog_only = catalog_cols.difference(model_cols)
    return model_only, catalog_only


def all_columns(
    catalog_columns: Dict[str, Any], model_columns: Dict[str, Any]
) -> Set[str]:
    catalog_cols = {col.lower() for col in catalog_columns.keys()}
    model_cols = {col.lower() for col in model_columns.keys()}
    return catalog_cols.union(model_cols)


def check_model_columns(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    catalog: Dict[str, Any],
    exclude_pattern: str,
    include_disabled: bool = False,
) -> Tuple[int, int, int]:
    paths = list(
        get_missing_file_paths(
            paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
        )
    )

    status_code = 0
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames, include_disabled=include_disabled)

    catalog_nodes = catalog.get("nodes", {})

    total = 0
    passed = 0

    for model in models:
        catalog_node = catalog_nodes.get(model.model_id, {})
        if catalog_node:
            model_columns = model.node.get("columns", {})
            catalog_columns = catalog_node.get("columns", {})

            this_total = len(
                all_columns(
                    catalog_columns=catalog_columns,
                    model_columns=model_columns,
                )
            )
            model_only, catalog_only = compare_columns(
                catalog_columns=catalog_columns,
                model_columns=model_columns,
            )
            total += this_total
            passed += this_total - len(catalog_only.union(model_only))
            schema_path = model.node.get("patch_path", "schema")  # pragma: no mutate
            if not schema_path:
                schema_path = "any .yml file"
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
    return status_code, total, passed


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

    start_time = time.time()
    status_code, total, passed = check_model_columns(
        paths=args.filenames,
        manifest=manifest,
        catalog=catalog,
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
            "description": "Check model has all columns",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    result_summary = ResultSummary(
        name=os.path.basename(__file__),
        description="Check model has all columns",
        status_code=status_code,
        total=total,
        passed=passed,
        execution_time=end_time - start_time,
    )
    ResultLogger(script_args=script_args).log_result(result_summary)

    return status_code


if __name__ == "__main__":
    exit(main())
