import argparse
from typing import Any
from typing import Dict
from typing import Optional
from typing import Sequence

from dbt_checkpoint.utils import (
    add_filenames_args,
    add_manifest_args,
    get_json,
    JsonOpenError,
    get_missing_file_paths,
    get_model_sqls,
    get_snapshots,
)

def validate_tags(
    paths: Sequence[str], manifest: Dict[str, Any]
) -> int:
    paths = get_missing_file_paths(paths, manifest)

    status_code = 0
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_snapshots(manifest)

    for model in models:
        # tags can be specified only from manifest
        raw_code = model.node.get("raw_code", [])
        if raw_code.find('target_schema=generate_schema_name') < 0:
            status_code = 1
            print(
                f"{model.node.get('original_file_path', model.filename)}: "
                f"is missing target_schema!",
            )
    return status_code


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

    return validate_tags(paths=args.filenames, manifest=manifest)


if __name__ == "__main__":
    exit(main())