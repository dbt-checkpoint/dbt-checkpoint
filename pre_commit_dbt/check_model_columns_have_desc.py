import argparse
import itertools
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_filenames
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_model_schemas
from pre_commit_dbt.utils import get_model_sqls
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import JsonOpenError
from pre_commit_dbt.utils import Model
from pre_commit_dbt.utils import ModelSchema


def check_column_desc(
    paths: Sequence[str], manifest: Dict[str, Any]
) -> Tuple[int, Dict[str, Any]]:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)
    # if user added schema but did not rerun the model
    schemas = get_model_schemas(list(ymls.values()), filenames)
    missing: Dict[str, Set[str]] = {}

    for item in itertools.chain(models, schemas):
        missing_cols = set()  # pragma: no mutate
        if isinstance(item, ModelSchema):
            model_name = item.model_name
            missing_cols = {
                key.get("name")
                for key in item.schema.get("columns", [])
                if not key.get("description")
            }
        # Model
        elif isinstance(item, Model):
            model_name = item.filename
            missing_cols = {
                key
                for key, value in item.node.get("columns", {}).items()
                if not value.get("description")
            }
        else:
            continue  # pragma: no cover, no mutate
        seen = missing.get(model_name)
        if seen:
            if not missing_cols:
                missing[model_name] = set()  # pragma: no mutate
            else:
                missing[model_name] = seen.union(missing_cols)
        elif missing_cols:
            missing[model_name] = missing_cols

    for model, columns in missing.items():
        if columns:
            status_code = 1
            result = "\n- ".join(list(columns))  # pragma: no mutate
            print(
                f"{sqls.get(model)}: "
                f"following columns are missing description:\n- {result}",
            )
    return status_code, missing


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    status_code, _ = check_column_desc(paths=args.filenames, manifest=manifest)
    return status_code


if __name__ == "__main__":
    exit(main())
