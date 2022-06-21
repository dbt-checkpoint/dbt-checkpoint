from pre_commit_dbt.utils import get_filenames, get_exposures
from typing import Sequence
from pathlib import Path
from typing import Optional
import argparse
from pre_commit_dbt.utils import add_filenames_args
import logging

    

def has_owner(paths: Sequence[str]):
    status_code = 0
    print(paths)
    ymls = get_filenames(paths, [".yml", ".yaml"])
    print(ymls)
    exposures = get_exposures(list(ymls.values()))
    for x in exposures:
        logging.debug(x)
    missing_owner = {exposure.exposure_name for exposure in exposures if "name" not in exposure.owner}
    logging.debug(missing_owner)
    for exposure in missing_owner:
        status_code = 1
        print(
            f"{exposure}: "
            f"does not have defined owner",
        )
    return status_code
    


def main(argv: Optional[Sequence[str]] = None) -> int :
    logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    args = parser.parse_args(argv)
    logging.debug(args.filenames)
    return has_owner(paths=args.filenames)


if __name__ == "__main__":
    exit(main())