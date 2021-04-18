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
from pre_commit_dbt.utils import get_macro_schemas
from pre_commit_dbt.utils import get_macro_sqls
from pre_commit_dbt.utils import get_macros
from pre_commit_dbt.utils import JsonOpenError
from pre_commit_dbt.utils import Macro
from pre_commit_dbt.utils import MacroSchema


def check_argument_desc(
    paths: Sequence[str], manifest: Dict[str, Any]
) -> Tuple[int, Dict[str, Any]]:
    status_code = 0
    ymls = get_filenames(paths, [".yml", ".yaml"])
    sqls = get_macro_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest macros that pre-commit found as changed
    macros = get_macros(manifest, filenames)
    # if user added schema but did not rerun the macro
    schemas = get_macro_schemas(list(ymls.values()), filenames)
    missing: Dict[str, Set[str]] = {}

    for item in itertools.chain(macros, schemas):
        missing_args = set()  # pragma: no mutate
        if isinstance(item, MacroSchema):
            macro_name = item.macro_name
            missing_args = {
                key.get("name")
                for key in item.schema.get("arguments", [])
                if not key.get("description")
            }
        # Macro
        elif isinstance(item, Macro):
            macro_name = item.filename
            missing_args = {
                key
                for key, value in item.macro.get("arguments", {}).items()
                if not value.get("description")
            }
        else:
            continue  # pragma: no cover, no mutate
        seen = missing.get(macro_name)
        if seen:
            if not missing_args:
                missing[macro_name] = set()  # pragma: no mutate
            else:
                missing[macro_name] = seen.union(missing_args)
        elif missing_args:
            missing[macro_name] = missing_args

    for macro, arguments in missing.items():
        if arguments:
            status_code = 1
            result = "\n- ".join(list(arguments))  # pragma: no mutate
            print(
                f"{sqls.get(macro)}: "
                f"following arguments are missing description:\n- {result}",
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

    status_code, _ = check_argument_desc(paths=args.filenames, manifest=manifest)
    return status_code


if __name__ == "__main__":
    exit(main())
