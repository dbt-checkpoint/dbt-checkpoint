import argparse
import os
import time
from pathlib import Path
from typing import Any
from typing import Dict
from typing import FrozenSet
from typing import Optional
from typing import Sequence

from yaml import dump
from yaml import safe_load

from pre_commit_dbt.check_script_ref_and_source import check_refs_sources
from pre_commit_dbt.tracking import dbtCheckpointTracking
from pre_commit_dbt.utils import add_default_args
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import JsonOpenError


def create_missing_sources(
    sources: Dict[FrozenSet[str], Dict[str, str]], output_path: str
) -> Dict:
    status_code = 0
    if sources:
        status_code = 1
        for _, source in sources.items():
            source_name = source.get("source_name")
            table_name = source.get("table_name")
            path = Path(output_path)
            # is file and exists
            if path.is_file():
                schema = safe_load(path.open())
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
                    dump(schema, f, default_flow_style=False, sort_keys=False)
            else:
                print(
                    f"Path `{output_path}` does not exists. "
                    f"Please create this file or change path."
                )
                return {"status_code": status_code}

    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

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

    start_time = time.time()
    check_refs_sources_properties = check_refs_sources(
        paths=args.filenames, manifest=manifest
    )

    sources = check_refs_sources_properties.get("sources")

    hook_properties = create_missing_sources(sources, output_path=args.schema_file)
    end_time = time.time()

    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "If any source is missing this hook tries to create it.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )
    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
