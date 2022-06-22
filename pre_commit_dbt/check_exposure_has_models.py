from pre_commit_dbt.utils import get_filenames, get_exposures
from typing import Sequence
from pathlib import Path
from typing import Optional
import argparse
from pre_commit_dbt.utils import add_filenames_args
import logging

def has_models(paths: Sequence[str]):
    status_code = 0
    print(paths)
    ymls = get_filenames(paths, [".yml", ".yaml"])
    print(ymls)
    exposures = get_exposures(list(ymls.values()))
    missing_model=[]
    for exposure in exposures:
        print(exposure)
        if exposure.models is None:
            missing_model.append(exposure.exposure_name)
    print(f"missing_model: {missing_model}")
    for exposure in missing_model:
        status_code = 1
        print(
            f"{exposure}: "
            f"does not have any model",
        )
    print(f"status_code: {status_code}")
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