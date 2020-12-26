import argparse
from typing import Optional
from typing import Sequence

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import get_filenames
from pre_commit_dbt.utils import get_manifest
from pre_commit_dbt.utils import get_models
from pre_commit_dbt.utils import ManifestOpenError


def find_model_properties(manifest: str, paths: Sequence[str]) -> int:
    status_code = 0
    filenames = get_filenames(paths)
    try:
        manifest_loaded = get_manifest(manifest)
    except ManifestOpenError as e:
        print(f"Unable to load manifest file ({e})")
        status_code = 1
        return status_code
    for model in get_models(manifest_loaded, filenames):
        if not model.get("patch_path"):
            print(
                f"{model.get('original_file_path')}: "
                f"does not have model properties defined.",
            )
            status_code = 1
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)

    args = parser.parse_args(argv)


if __name__ == "__main__":
    exit(main())
