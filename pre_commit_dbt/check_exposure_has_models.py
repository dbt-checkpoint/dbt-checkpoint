from pre_commit_dbt.utils import get_filenames, get_exposures
from typing import Sequence
from pathlib import Path
from typing import Optional
import argparse
from pre_commit_dbt.utils import add_filenames_args
import logging

def has_models(paths: Sequence[str]):
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    exposures = get_exposures(list(ymls.values()))
    missing_model = { exposure.exposure_name for exposure in exposures if exposure.models is None}
    for exposure in missing_model:
        status_code = 1
        print(
            f"{exposure}: "
            f"does not have any model",
        )
    return status_code



def main(argv: Optional[Sequence[str]] = None) -> int :
    logging.basicConfig(filename='example.log', encoding='utf-8', level=logging.DEBUG)
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    args = parser.parse_args(argv)
    logging.debug(args.filenames)
    return has_models(paths=args.filenames)


if __name__ == "__main__":
    exit(main())