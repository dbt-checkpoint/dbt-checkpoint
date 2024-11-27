import argparse
import json
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Sequence, Set, Text, Union


from yaml import safe_load

DEFAULT_MANIFEST_PATH = "target/manifest.json"
DEFAULT_CATALOG_PATH = "target/catalog.json"


class CalledProcessError(RuntimeError):
    pass


class JsonOpenError(RuntimeError):
    pass


class CompilationException(RuntimeError):
    pass


@dataclass
class Model:
    model_id: str
    model_name: str
    filename: str
    node: Dict[str, Any]


@dataclass
class Macro:
    macro_id: str
    macro_name: str
    filename: str
    macro: Dict[str, Any]


@dataclass
class Test:
    test_id: str
    test_type: str
    test_name: str
    node: Dict[str, Any]


@dataclass
class Source:
    source_id: str
    source_name: str
    table_name: str
    filename: str
    node: Dict[str, Any]


@dataclass
class ModelSchema:
    model_name: str
    filename: str
    schema: Dict[str, Any]
    file: Path
    prefix: str = "model"


@dataclass
class MacroSchema:
    macro_name: str
    filename: str
    schema: Dict[str, Any]
    file: Path
    prefix: str = "macro"


@dataclass
class SourceSchema:
    source_name: str
    table_name: str
    filename: str
    source_schema: Dict[str, Any]
    table_schema: Dict[str, Any]
    prefix: str = "source"


@dataclass
class GenericDbtObject:
    name: str
    filename: str
    schema: Dict[str, Any]


def cmd_output(
    *cmd: str,
    expected_code: Optional[int] = 0,
    **kwargs: Any,
) -> str:
    kwargs.setdefault("stdout", subprocess.PIPE)
    kwargs.setdefault("stderr", subprocess.PIPE)
    proc = subprocess.Popen(cmd, **kwargs)
    stdout, stderr = proc.communicate()
    stdout = stdout.decode()
    if expected_code is not None and proc.returncode != expected_code:
        raise CalledProcessError(
            cmd,
            expected_code,
            proc.returncode,
            stdout,
            stderr,
        )
    return stdout


def paths_to_dbt_models(
    paths: Sequence[str],
    prefix: str = "",
    postfix: str = "",
) -> List[str]:
    return [prefix + Path(path).stem + postfix for path in paths]


def checkpoint_safe_load(stream):
    # FIXME: temporary fix for YAML incompatibility of safe_load with empty files
    return safe_load(stream) or {}


def get_json(json_filename: str) -> Dict[str, Any]:
    try:
        file_content = Path(json_filename).read_text(encoding="utf-8")
        return json.loads(file_content)
    except Exception as e:
        raise JsonOpenError(e)


def get_config_file(config_file_path: str) -> Dict[str, Any]:
    try:
        path = Path(config_file_path)
        if not path.exists():
            alt_path = path.with_suffix(".yml" if path.suffix == ".yaml" else ".yaml")
            if alt_path.exists():
                path = alt_path
        config = checkpoint_safe_load(path.open())
        check_yml_version(config_file_path, config)
    except FileNotFoundError:
        config = {}
    return config


def get_models(
    manifest: Dict[str, Any],
    filenames: Set[str],
    include_ephemeral: bool = False,
    include_disabled: bool = False,
) -> Generator[Model, None, None]:
    nodes = manifest.get("nodes", {})
    for key, node in nodes.items():
        # Ephemeral models break many tests and should be wholly excluded,
        # someone can make an argument for their inclusion on a case by case basis
        # in which case we would pass `include_ephemeral`
        if (
            not include_ephemeral
            and node.get("config", {}).get("materialized") == "ephemeral"
        ):
            continue
        # In case a disabled model is still in `nodes`
        if not include_disabled and not node.get("config", {}).get("enabled", True):
            continue
        split_key = key.split(".")
        # Versions are supported since dbt-core 1.5
        if node.get("version") and split_key[-1] == f"v{node.get('version')}":
            # dbt versioned filenames can be either `model_name` or `model_name_v{version}`
            filename_candidates = [
                f"{split_key[-2]}",
                f"{split_key[-2]}_v{node.get('version')}",
            ]
        else:
            filename_candidates = [split_key[-1]]
        if split_key[0] == "model":
            for fn in filename_candidates:
                if fn in filenames:
                    yield Model(key, node.get("name"), fn, node)  # pragma: no mutate


def get_ephemeral(
    manifest: Dict[str, Any],
) -> List[str]:
    output = []
    nodes = manifest.get("nodes", {})
    for key, node in nodes.items():
        if not node.get("config", {}).get("materialized") == "ephemeral":
            continue
        split_key = key.split(".")
        filename = split_key[-1]
        if split_key[0] == "model":
            output.append(filename)
    return output


def get_snapshot_filenames(
    manifest: Dict[str, Any],
) -> List[str]:
    output = []
    nodes = manifest.get("nodes", {})
    for key, node in nodes.items():  # pragma: no cover
        if not node.get("config", {}).get("materialized") == "snapshot":
            continue
        split_key = key.split(".")
        filename = split_key[-1]
        if split_key[0] == "snapshot":
            output.append(filename)
    return output


def get_snapshots(
    manifest: Dict[str, Any], filenames: Set[str]
) -> Generator[GenericDbtObject, None, None]:
    nodes = manifest.get("nodes", {})
    for key, node in nodes.items():
        if not node.get("config", {}).get("materialized") == "snapshot":
            continue
        split_key = key.split(".")
        filename = split_key[-1]
        if filename in filenames and split_key[0] == "snapshot":
            yield GenericDbtObject(
                node.get("name"), filename, node
            )  # pragma: no mutate


def get_tests(
    manifest: Dict[str, Any], filenames: Set[str]
) -> Generator[GenericDbtObject, None, None]:
    nodes = manifest.get("nodes", {})
    for key, node in nodes.items():
        if not node.get("config", {}).get("materialized") == "test":
            continue
        split_key = key.split(".")
        filename = split_key[-1]
        if filename in filenames and split_key[0] == "test":
            yield GenericDbtObject(
                node.get("name"), filename, node
            )  # pragma: no mutate


def get_macros(
    manifest: Dict[str, Any],
    filenames: Set[str],
) -> Generator[Macro, None, None]:
    macros = manifest.get("macros", {})
    for key, macro in macros.items():
        split_key = key.split(".")
        filename = split_key[-1]
        if filename in filenames and split_key[0] == "macro":
            yield Macro(key, macro.get("name"), filename, macro)  # pragma: no mutate


def get_seeds(
    manifest: Dict[str, Any],
    filenames: Set[str],
) -> Generator[GenericDbtObject, None, None]:
    seeds = manifest.get("nodes", {})
    for key, seed in seeds.items():
        split_key = key.split(".")
        filename = split_key[-1]
        if filename in filenames and split_key[0] == "seed":
            yield GenericDbtObject(
                seed.get("name"), filename, seed
            )  # pragma: no mutate


def get_flags(flags: Optional[Sequence[str]] = None) -> List[str]:
    if flags:
        return [flag.replace("+", "-") for flag in flags if flag]
    else:
        return []


def get_macro_sqls(paths: Sequence[str], manifest: Dict[str, Any]) -> Dict[str, Path]:
    sqls = get_filenames(paths, [".sql"])
    macro_paths = [m["path"] for m in manifest.get("macros", {}).values()]
    macro_sqls = get_filenames(macro_paths, extensions=[".sql"])
    return {k: v for k, v in sqls.items() if k in macro_sqls and v == macro_sqls[k]}


def get_disabled(manifest: Dict[str, Any], include_disabled: bool = False) -> List[str]:
    output = []
    disabled = manifest.get("disabled", {})
    for key, node in disabled.items():
        split_key = key.split(".")
        filename = split_key[-1]
        if split_key[0] == "model":
            if include_disabled:
                continue
            output.append(filename)

    return output


def get_model_sqls(
    paths: Sequence[str], manifest: Dict[str, Any], include_disabled: bool = False
) -> Dict[str, Any]:
    ephemeral = get_ephemeral(manifest)
    sqls = get_filenames(paths, [".sql"])
    macro_sqls = get_macro_sqls(paths, manifest)
    disabled = get_disabled(manifest, include_disabled)
    return {
        k: v
        for k, v in sqls.items()
        if k not in macro_sqls and k not in ephemeral and k not in disabled
    }


def get_model_schemas(
    yml_files: Sequence[Path], filenames: Set[str], all_schemas: bool = False
) -> Generator[ModelSchema, None, None]:
    for yml_file in yml_files:
        with open(yml_file, "r") as file:
            schema = checkpoint_safe_load(file)
            for model in schema.get("models", []):
                if isinstance(model, dict) and model.get("name"):
                    model_name = model.get("name", "")  # pragma: no mutate
                    if model_name in filenames or all_schemas:
                        yield ModelSchema(
                            model_name=model_name,
                            file=yml_file,
                            filename=yml_file.stem,
                            schema=model,
                        )


def get_macro_schemas(
    yml_files: Sequence[Path], filenames: Set[str], all_schemas: bool = False
) -> Generator[MacroSchema, None, None]:
    for yml_file in yml_files:
        schema = checkpoint_safe_load(yml_file.open())
        for macro in schema.get("macros", []):
            if isinstance(macro, dict) and macro.get("name"):
                macro_name = macro.get("name", "")  # pragma: no mutate
                if macro_name in filenames or all_schemas:
                    yield MacroSchema(
                        macro_name=macro_name,
                        file=yml_file,
                        filename=yml_file.stem,
                        schema=macro,
                    )


def get_source_schemas(
    yml_files: Sequence[Path], include_disabled: bool = False
) -> Generator[SourceSchema, None, None]:
    for yml_file in yml_files:
        schema = checkpoint_safe_load(yml_file.open())
        for source in schema.get("sources", []):
            if not include_disabled and not source.get("config", {}).get(
                "enabled", True
            ):
                continue
            source_name = source.get("name")
            tables = source.pop("tables", [])
            for table in tables:
                table_name = table.get("name")
                yield SourceSchema(
                    source_name=source_name,
                    table_name=table_name,
                    filename=yml_file.stem,
                    source_schema=source,
                    table_schema=table,
                )


def get_exposures(
    yml_files: Sequence[Path],
) -> Generator[GenericDbtObject, None, None]:
    for yml_file in yml_files:
        schema = checkpoint_safe_load(yml_file.open())
        for exposure in schema.get("exposures", []):
            exposure_name = exposure.get("name")
            yield GenericDbtObject(
                name=exposure_name,
                filename=yml_file.stem,
                schema=exposure,
            )


def obj_in_deps(obj: Any, dep_name: str) -> bool:
    dep_split = set(dep_name.split("."))
    result = False
    if isinstance(obj, SourceSchema):
        result = {obj.prefix, obj.source_name, obj.table_name}.issubset(dep_split)
    elif isinstance(obj, ModelSchema):
        result = {obj.prefix, obj.model_name}.issubset(dep_split)
    elif isinstance(obj, Model):
        result = obj.model_id == dep_name
    return result


def get_test(node_id: str, manifest: Dict[str, Any]) -> Test:
    test_node = manifest.get("nodes", {}).get(node_id)
    test_type = "data" if "data" in test_node.get("tags", []) else "schema"
    test_name = test_node.get("test_metadata", {}).get("name") or "data"
    return Test(
        test_id=node_id,
        test_type=test_type,
        test_name=test_name,
        node=test_node,
    )


def get_parent_childs(
    manifest: Dict[str, Any], obj: Any, manifest_node: str, node_types: List[str]
) -> Generator[Union[Test, Model, Source], None, None]:
    deps = manifest.get(manifest_node, {})
    for dep_name, dep_items in deps.items():
        if obj_in_deps(obj, dep_name):
            for node_id in dep_items:
                node_type = node_id.split(".")[0]
                if node_type in node_types:
                    if node_type == "test":
                        yield get_test(node_id, manifest)
                    elif node_type == "model":
                        node = manifest.get("nodes", {}).get(node_id)
                        yield Model(
                            model_id=node_id,
                            model_name=node.get("name", ""),  # pragma: no mutate
                            filename=node.get("path", ""),  # pragma: no mutate
                            node=node,
                        )
                    else:  # Source
                        node = manifest.get("sources", {}).get(node_id)
                        yield Source(
                            source_id=node_id,
                            source_name=node.get(
                                "source_name", ""
                            ),  # pragma: no mutate
                            table_name=node.get("name", ""),  # pragma: no mutate
                            filename=node.get("path", ""),  # pragma: no mutate
                            node=node,
                        )


def get_filenames(
    paths: Sequence[str],
    extensions: Optional[Sequence[str]] = None,
) -> Dict[str, Path]:
    result = {}
    for path in paths:
        file = Path(path)
        filename = file.stem
        if extensions and file.suffix not in extensions:
            pass
        else:
            result[filename] = file
    return result


def run_dbt_cmd(cmd: Sequence[Any]) -> int:
    status_code = 0
    print(f"Executing cmd: `{' '.join(cmd)}`")
    try:
        output = cmd_output(*list(filter(None, cmd)), expected_code=0)
        print(output)
    except CalledProcessError as e:
        print(e.args[3])  # pragma: no mutate
        status_code = 1
        return status_code
    return status_code


def get_manifest_node_from_file_path(
    manifest: Dict[str, Any], file_path: str
) -> Dict[str, Any]:
    for node in manifest.get("nodes", {}).values():
        if node.get("original_file_path", "") in file_path:
            return node
    return {}


def add_filenames_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "filenames",
        nargs="*",
        help="Filenames pre-commit believes are changed.",
    )


def add_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        type=str,
        default=".dbt-checkpoint.yaml",
        help="""Location of .dbt-checkpoint.yaml. Usually at the dbt root directory.
        This file contains the global config for dbt-checkpoint.
        """,
    )


def add_manifest_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--manifest",
        type=str,
        default=DEFAULT_MANIFEST_PATH,
        help="""Location of manifest.json file. Usually target/manifest.json.
        This file contains a full representation of dbt project.
        """,
    )


def add_catalog_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--catalog",
        type=str,
        default=DEFAULT_CATALOG_PATH,
        help="""Location of catalog.json file. Usually target/catalog.json.
        dbt uses this file to render information like column types and table
        statistics into the docs site. In pre-commit-dbt is used for columns
        operations.
        """,
    )


def add_tracking_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--is_test",
        action="store_true",
        help="True the execution is a test.",
    )


def add_exclude_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--exclude",
        type=str,
        default="",
        help="Pattern to exclude files from missing filepath discovery",
    )


def add_disabled_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--include-disabled",
        action="store_true",
        help="Flagto include disabled models",
    )


def add_default_args(parser: argparse.ArgumentParser) -> None:
    add_filenames_args(parser)
    add_manifest_args(parser)
    add_config_args(parser)
    add_tracking_args(parser)
    add_exclude_args(parser)
    add_disabled_args(parser)


def add_dbt_cmd_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--global-flags",
        nargs="*",
        help="""Global dbt flags applicable to all subcommands.
        Instead of dash `-` please use `+`.""",
    )
    parser.add_argument(
        "--cmd-flags",
        nargs="*",
        help="Command-specific dbt flags. Instead of dash `-` please use `+`.",
    )


def add_dbt_cmd_model_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--model-prefix",
        type=str,
        default="",
        help="Prefix dbt selector, for selecting parents.",
    )
    parser.add_argument(
        "--model-postfix",
        type=str,
        default="",
        help="Postfix dbt selector, for selecting children.",
    )
    parser.add_argument(
        "--models",
        nargs="*",
        help="""pre-commit-dbt is by default running changed files.
        If you need to override that, e.g. in case of Slim CI (state:modified),
        you can use this option.""",
    )


def add_meta_keys_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--meta-keys",
        nargs="+",
        required=True,
        help="List of required key in meta part of model.",
    )
    parser.add_argument(
        "--allow-extra-keys",
        action="store_true",
        required=False,
        help="Whether extra keys are allowed.",
    )


def check_yml_version(file_path: str, yaml_dct: Dict[str, Any]) -> None:
    if "version" not in yaml_dct:
        raise_invalid_property_yml_version(
            file_path,
            "the yml property file {} is missing a version tag".format(file_path),
        )

    version = yaml_dct["version"]
    # if it's not an integer, the version is malformed, or not
    # set. Either way, only 'version: 2' is supported.
    if not isinstance(version, int):
        raise_invalid_property_yml_version(
            file_path,
            "its 'version:' tag must be an integer (e.g. version: 2)."
            " {} is not an integer".format(version),
        )
    if version != 1:
        raise_invalid_property_yml_version(
            file_path,
            "its 'version:' tag is set to {}.  Only 1 is supported".format(version),
        )


def raise_invalid_property_yml_version(path: str, issue: str) -> None:
    # TODO: URL AS PLACEHOLDER - LINK TO THE DOC SECTION ON dbt-checkpoint CONFIG
    # WHEN AVAILABLE
    raise CompilationException(
        "The yml property file at {} is invalid because {}. Please consult the "
        "documentation for more information on yml property file syntax:\n\n"
        "https://github.com/dbt-checkpoint/dbt-checkpoint/blob/main/HOOKS.md#check-column-desc-are-same".format(  # noqa: E501, line length
            path, issue
        )
    )


class ParseDict(argparse.Action):
    """Parse a KEY=VALUE string-list into a dictionary"""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Union[Text, Sequence[Any], None],
        option_string: Optional[str] = None,
    ) -> None:
        """Perform the parsing"""
        result = {}

        if values:
            for item in values:
                split_items = item.split("=", 1)  # pragma: no mutate
                key = split_items[0].strip()
                value = split_items[1]

                result[key] = value

        setattr(namespace, self.dest, result)


class ParseJson(argparse.Action):
    """Parse a JSON string into a dictionary"""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Text,
        option_string: Optional[str] = None,
    ) -> None:
        """Perform the parsing"""
        result = {}

        if values:
            result = json.loads(values)

        setattr(namespace, self.dest, result)


def add_related_sqls(
    yml_path: str,
    nodes: Dict[Any, Any],
    paths_with_missing: Set[str],
    include_ephemeral: bool = False,
) -> None:
    yml_path_class = Path(yml_path)
    yml_path_parts = list(yml_path_class.parts)
    # Remove the first 'project' component
    yml_path_parts.pop(0)
    dbt_patch_path = "/".join(yml_path_parts)

    for key, node in nodes.items():
        if (
            not include_ephemeral
            and node.get("config", {}).get("materialized") == "ephemeral"
        ):
            continue

        if node.get("patch_path") and dbt_patch_path in node.get("patch_path"):
            if ".sql" in node.get("original_file_path", "").lower():
                for related_sql_file in _discover_sql_files(node):
                    sql_as_string = related_sql_file.as_posix()
                    if "target/" not in sql_as_string.lower():
                        paths_with_missing.add(sql_as_string)


def add_related_ymls(
    sql_path: str,
    nodes: Dict[Any, Any],
    paths_with_missing: Set[str],
    include_ephemeral: bool = False,
) -> None:
    for key, node in nodes.items():
        if (
            not include_ephemeral
            and node.get("config", {}).get("materialized") == "ephemeral"
        ):
            continue

        if node.get("path") and (node.get("path") in sql_path):
            patch_path = node.get("patch_path", None)
            if patch_path:
                # Original patch_path has 'project\\path\to\yml.yml'
                # Remove `project_name\\` from patch_path
                patch_path = Path(patch_path)
                clean_patch_path = patch_path.relative_to(
                    *patch_path.parts[:1]
                ).as_posix()
                for related_yml_file in _discover_prop_files(clean_patch_path):
                    yml_as_string = related_yml_file.as_posix()
                    if "target/" not in yml_as_string.lower():
                        paths_with_missing.add(yml_as_string)


def _discover_sql_files(node):  # type: ignore
    return Path().glob(f"**/{node.get('original_file_path')}")


def _discover_prop_files(model_path):  # type: ignore
    return Path().glob(f"**/{model_path}")


def get_missing_file_paths(
    paths: Sequence[str],
    manifest: Dict[Any, Any] = {},
    include_ephemeral: bool = False,
    extensions: Sequence[str] = [".sql", ".yml", ".yaml"],
    exclude_pattern: str = "",
) -> Set[str]:
    nodes = manifest.get("nodes", {})
    paths_with_missing = set(paths)
    if nodes:
        for path in paths:
            suffix = Path(path).suffix.lower()
            if suffix == ".sql" and (".yml" in extensions or ".yaml" in extensions):
                add_related_ymls(path, nodes, paths_with_missing, include_ephemeral)
            elif (suffix == ".yml" or suffix == ".yaml") and ".sql" in extensions:
                add_related_sqls(path, nodes, paths_with_missing, include_ephemeral)
            else:
                continue
    if exclude_pattern:
        exclude_re = re.compile(exclude_pattern)
        paths_with_missing = [  # type: ignore
            filename
            for filename in paths_with_missing
            if not exclude_re.search(filename)
        ]
    file_paths_with_missing = [p for p in paths_with_missing if not os.path.isdir(p)]
    return file_paths_with_missing


def red(string: Optional[Any]) -> str:
    return "\033[91m" + str(string) + "\033[0m"


def yellow(string: Optional[Any]) -> str:
    return "\033[93m" + str(string) + "\033[0m"


def extend_dbt_project_dir_flag(
    cmd: List[str], cmd_flags: List[str], dbt_project_dir: str = ""
) -> List[str]:
    if dbt_project_dir and not "--project-dir" in cmd_flags:  # noqa
        cmd.extend(["--project-dir", dbt_project_dir])
    return cmd


def get_dbt_manifest(args):  # type: ignore
    """
    Get dbt manifest following the new config file approach. Precedence:
        - custom `--manifest` flag
        - .dbt-checkpoint.yaml `dbt-project-dir` key
        - default `--manifest` flag
    """
    manifest_path = args.manifest
    dbt_checkpoint_config = get_config_file(args.config)
    config_project_dir = dbt_checkpoint_config.get("dbt-project-dir")
    if manifest_path != DEFAULT_MANIFEST_PATH:
        return get_json(manifest_path)
    elif config_project_dir:
        return get_json(f"{config_project_dir}/target/manifest.json")
    else:
        return get_json(manifest_path)


def get_dbt_catalog(args):  # type: ignore
    """
    Get dbt catalog following the new config file approach
    """
    catalog_path = args.catalog
    dbt_checkpoint_config = get_config_file(args.config)
    config_project_dir = dbt_checkpoint_config.get("dbt-project-dir")
    if catalog_path != DEFAULT_CATALOG_PATH:
        return get_json(catalog_path)
    elif config_project_dir:
        return get_json(f"{config_project_dir}/target/catalog.json")
    else:
        return get_json(catalog_path)


def validate_meta_keys(
    obj: GenericDbtObject,
    meta_keys: Sequence[str],
    meta_set: Set,
    allow_extra_keys: bool,
):
    meta = set(obj.schema.get("meta", {}).keys())
    if allow_extra_keys:
        diff = not meta_set.issubset(meta)
    else:
        diff = not (meta_set == meta)
    if diff:
        print(
            f"{obj.name} meta keys don't match. \n"
            f"Provided: {yellow(', '.join(list(meta_keys)))}\n"
            f"Actual: {red(', '.join(list(meta)))}\n"
        )
        return 1
    return 0


def strings_differ_in_case(str1: str, str2: str) -> bool:
    return str1.lower() == str2.lower() and str1 != str2
