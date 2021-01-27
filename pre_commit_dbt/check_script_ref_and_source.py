import argparse
import re
from typing import Any
from typing import Dict
from typing import FrozenSet
from typing import Optional
from typing import Sequence
from typing import Set
from typing import Tuple

from pre_commit_dbt.utils import add_filenames_args
from pre_commit_dbt.utils import add_manifest_args
from pre_commit_dbt.utils import get_filenames
from pre_commit_dbt.utils import get_json
from pre_commit_dbt.utils import JsonOpenError


def check_refs_sources(
    paths: Sequence[str], manifest: Dict[str, Any]
) -> Tuple[int, Set[str], Dict[FrozenSet[str], Dict[str, str]]]:
    status_code = 0
    sqls = get_filenames(paths, [".sql"])

    models = set()
    sources = {}
    for _, file in sqls.items():
        full_script = file.read_text(encoding="utf-8")
        src_refs = re.findall(r"\{\{\s*(source|ref)\s*\((.*)\)\s*\}\}", full_script)
        for src_ref in src_refs:
            src_ref_value = src_ref[1].replace("'", "").replace('"', "").strip()
            if src_ref[0] == "ref":
                models.add(src_ref_value)
            if src_ref[0] == "source":
                src_split = src_ref_value.split(",")
                source_name = src_split[0].strip()
                table_name = src_split[1].strip()
                src_key = frozenset([source_name, table_name])
                sources[src_key] = {
                    "source_name": source_name,
                    "table_name": table_name,
                }

    if models:
        nodes = manifest.get("nodes", {})
        for _, value in nodes.items():
            model_name = value.get("name")
            if model_name in models:
                models.remove(model_name)

    if sources:
        srcs = manifest.get("sources", {})
        for _, value in srcs.items():
            source_set = frozenset([value.get("source_name"), value.get("name")])
            if source_set in sources.keys():
                sources.pop(source_set)

    for _, src in sources.items():
        status_code = 1
        source_name = src.get("source_name")  # pragma: no mutate
        table_name = src.get("table_name")  # pragma: no mutate
        print(f"Missing source `{source_name}.{table_name}`")

    for missing_ref in models:
        status_code = 1
        print(f"Missing model (ref) {missing_ref}")

    return status_code, models, sources


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

    status_code, _, _ = check_refs_sources(paths=args.filenames, manifest=manifest)
    return status_code


if __name__ == "__main__":
    exit(main())
