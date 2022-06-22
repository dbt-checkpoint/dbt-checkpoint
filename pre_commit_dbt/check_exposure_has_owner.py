from pre_commit_dbt.utils import get_filenames, get_exposures
from typing import Sequence
from pathlib import Path
from typing import Optional
import argparse
from pre_commit_dbt.utils import add_filenames_args

    

def has_owner(paths: Sequence[str]):
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    exposures = get_exposures(list(ymls.values()))
    missing_owner = {exposure.exposure_name for exposure in exposures if "name" not in exposure.owner}
    for exposure in missing_owner:
        status_code = 1
        print(
            f"{exposure}: "
            f"does not have defined owner",
        )
    return status_code
    


def main(argv: Optional[Sequence[str]] = None) -> int :
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    args = parser.parse_args(argv)
    return has_owner(paths=args.filenames)


if __name__ == "__main__":
    exit(main())