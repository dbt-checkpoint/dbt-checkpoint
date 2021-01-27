import argparse
from pathlib import Path
from typing import Dict
from typing import FrozenSet
from typing import Optional
from typing import Sequence

import yaml

from pre_commit_dbt.check_script_ref_and_source import check_refs_sources
from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import JsonOpenError


def create_missing_sources(
    sources: Dict[FrozenSet[str], Dict[str, str]], output_path: str
) -> int:
    status_code = 0
    if sources:
        status_code = 1
        for _, source in sources.items():
            source_name = source.get("source_name")
            table_name = source.get("table_name")
            path = Path(output_path)
            # is file and exists
            if path.is_file():
                schema = yaml.safe_load(path.open())
                schema_sources = schema.get("sources", [])
                seen = False  # pragma: no mutate
                for schema_source in schema_sources:
                    if schema_source.get("name") == source_name:
                        seen = True
                        tables = schema_source.setdefault("tables", [])
                        tables.append({"name": table_name})
                if not seen:  # pragma: no mutate
                    print(
                        f"Source `{source_name}` does not "
                        f"exists in `{output_path}`. Please create it "
                        f"before adding tables."
                    )
                with open(path, "w") as f:
                    print(f"Generating missing source `{source_name}.{table_name}`.")
                    yaml.dump(schema, f, default_flow_style=False, sort_keys=False)
            else:
                print(
                    f"Path `{output_path}` does not exists. "
                    f"Please create this file or change path."
                )
                return status_code

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_filenames_args(parser)
    add_manifest_args(parser)

    parser.add_argument(
        "--schema-file",
        required=True,
        type=str,
        help="""Location of schema.yml file. Where new source tables should
        be created.
        """,
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_json(args.manifest)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    _, _, sources = check_refs_sources(paths=args.filenames, manifest=manifest)

    status_code = create_missing_sources(sources, output_path=args.schema_file)

    return status_code


if __name__ == "__main__":
    exit(main())
