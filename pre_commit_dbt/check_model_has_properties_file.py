import argparse
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import get_model_sqls
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import JsonOpenError


def has_properties_file(
    paths: Sequence[str], manifest: Dict[str, Any]
) -> Tuple[int, Set[str]]:
    status_code = 0
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)
    # convert to sets
    in_models = {model.filename for model in models if model.node.get("patch_path")}
    missing = filenames.difference(in_models)

    for model in missing:
        status_code = 1
        print(
            f"{sqls.get(model)}: "
            f"does not have model properties defined in any .yml file.",
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

    status_code, _ = has_properties_file(paths=args.filenames, manifest=manifest)
    return status_code


if __name__ == "__main__":
    exit(main())
