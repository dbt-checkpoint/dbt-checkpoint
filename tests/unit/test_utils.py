from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from jinja2 import Environment

from dbt_checkpoint.utils import CalledProcessError
from dbt_checkpoint.utils import check_yml_version
from dbt_checkpoint.utils import cmd_output
from dbt_checkpoint.utils import CompilationException
from dbt_checkpoint.utils import extend_dbt_project_dir_flag
from dbt_checkpoint.utils import get_dbt_catalog
from dbt_checkpoint.utils import get_dbt_manifest
from dbt_checkpoint.utils import get_filenames
from dbt_checkpoint.utils import get_macro_args_from_sql_code
from dbt_checkpoint.utils import get_macro_schemas
from dbt_checkpoint.utils import get_missing_file_paths
from dbt_checkpoint.utils import get_model_schemas
from dbt_checkpoint.utils import get_path_relative_to_dbt_project_dir
from dbt_checkpoint.utils import Jinja2TestMacroExtension
from dbt_checkpoint.utils import MacroSchema
from dbt_checkpoint.utils import Model
from dbt_checkpoint.utils import ModelSchema
from dbt_checkpoint.utils import obj_in_deps
from dbt_checkpoint.utils import paths_to_dbt_models
from dbt_checkpoint.utils import SourceSchema


def test_cmd_output_error():
    with pytest.raises(CalledProcessError):
        cmd_output("sh", "-c", "exit 1")


def test_cmd_output_output():
    ret = cmd_output("echo", "hi")
    assert ret == "hi\n"


@pytest.mark.parametrize(
    "test_input,pre,post,expected",
    [
        (["/aa/bb/cc.txt", "ee"], "", "", ["cc", "ee"]),
        (["/aa/bb/cc.txt"], "+", "", ["+cc"]),
        (["/aa/bb/cc.txt"], "", "+", ["cc+"]),
        (["/aa/bb/cc.txt"], "+", "+", ["+cc+"]),
    ],
)
def test_paths_to_dbt_models(test_input, pre, post, expected):
    result = paths_to_dbt_models(test_input, pre, post)
    assert result == expected


def test_paths_to_dbt_models_default():
    result = paths_to_dbt_models(["/aa/bb/cc.txt", "ee"])
    assert result == ["cc", "ee"]


@pytest.mark.parametrize(
    "obj,child_name,result",
    [
        (Model("aa", "bb", "cc", {}), "aa", True),
        (SourceSchema("aa", "bb", "cc", {}, {}), "source.aa.bb", True),
        (ModelSchema("aa", "model.aa", {}, Path("model.aa")), "model.aa", True),
        (object, "model.aa", False),
    ],
)
def test_obj_in_child(obj, child_name, result):
    assert obj_in_deps(obj, child_name) == result


def test_get_filenames():
    result = get_filenames(["aa/bb/cc.sql", "bb/ee.sql"])
    assert result == {"cc": Path("aa/bb/cc.sql"), "ee": Path("bb/ee.sql")}


def test_get_model_schemas(tmpdir):
    sub = tmpdir.mkdir("sub")
    file = sub.join("schema.yml")
    file.write(
        """
version: 2

models:
    name: aaa
    description: bbb
    """
    )
    result = get_model_schemas([file], {"aa"})
    assert list(result) == []


def test_get_macro_schemas_malformed_schema(tmpdir):
    sub = tmpdir.mkdir("sub")
    file = sub.join("schema.yml")
    file.write(
        """
version: 2

macros:
    name: aaa
    description: bbb
    """
    )

    result = get_macro_schemas([Path(file)], {"aa"})
    assert list(result) == []


def test_get_macro_schemas_correct_schema(tmpdir):
    sub = tmpdir.mkdir("sub")
    file = sub.join("schema.yml")
    file.write(
        """
version: 2

macros:
    - name: aaa
      description: bbb
    """
    )
    result = get_macro_schemas([Path(file)], {"aaa"})
    assert list(result) == [
        MacroSchema(
            macro_name="aaa",
            filename="schema",
            schema={"name": "aaa", "description": "bbb"},
            file=Path(file),
            prefix="macro",
        )
    ]


def test_check_yml_version_without_version():
    yaml_dct = {"test": "test"}
    with pytest.raises(CompilationException):
        check_yml_version("file_path", yaml_dct)


def test_check_yml_version_with_non_int_version():
    yaml_dct = {"version": "test"}
    with pytest.raises(CompilationException):
        check_yml_version("file_path", yaml_dct)


def test_check_yml_version_with_non_1_version():
    yaml_dct = {"version": 2}
    with pytest.raises(CompilationException):
        check_yml_version("file_path", yaml_dct)


@pytest.mark.parametrize(
    (
        "input_files",
        "discovered_files",
        "valid_manifest",
        "exclude_regex",
        "expected_files",
    ),
    (
        (
            ["aa/bb/with_description.sql"],
            "bb/with_description.yml",
            True,
            "",
            ["aa/bb/with_description.sql", "bb/with_description.yml"],
        ),
        (
            ["aa/bb/with_description.sql"],
            "",
            False,
            "",
            ["aa/bb/with_description.sql"],
        ),
        (
            ["aa/bb/with_description.sql"],
            "aa/bb/with_description.yml",
            True,
            r"^(.+)\/([^\/]+)$",
            [],
        ),
        (
            ["aa/bb/with_description.yml"],
            "bb/with_description.sql",
            True,
            "",
            ["aa/bb/with_description.yml", "bb/with_description.sql"],
        ),
    ),
)
def test_get_missing_file_paths(
    input_files,
    discovered_files,
    valid_manifest,
    exclude_regex,
    expected_files,
    manifest_path_str,
):
    manifest = {}
    if valid_manifest:
        import json

        manifest = json.load(open(manifest_path_str))
    with patch("dbt_checkpoint.utils._discover_prop_files") as mock_discover_prop_files:
        with patch(
            "dbt_checkpoint.utils._discover_sql_files"
        ) as mock_discover_sql_files:
            mock_discover_sql_files.return_value = [Path(discovered_files)]
            mock_discover_prop_files.return_value = [Path(discovered_files)]
            resulting_files = get_missing_file_paths(
                paths=input_files, manifest=manifest, exclude_pattern=exclude_regex
            )
    assert set(resulting_files) == set(expected_files)


def test_extend_dbt_cmd_flags_with_project_dir():
    cmd = ["dbt", "run"]
    cmd_flags = ["--target", "dev"]
    dbt_project_dir = "/path/to/project"

    expected_result = ["dbt", "run", "--project-dir", "/path/to/project"]
    result = extend_dbt_project_dir_flag(cmd, cmd_flags, dbt_project_dir)

    assert result == expected_result


def test_extend_dbt_cmd_flags_without_project_dir():
    cmd = ["dbt", "run"]
    cmd_flags = ["--target", "dev"]
    dbt_project_dir = ""
    expected_result = ["dbt", "run"]
    result = extend_dbt_project_dir_flag(cmd, cmd_flags, dbt_project_dir)
    assert result == expected_result


def test_get_dbt_manifest_with_config_project_dir():
    class Args:
        manifest = "target/manifest.json"
        config = ".dbt_checkpoint.yaml"

    with patch("dbt_checkpoint.utils.get_config_file") as mock_get_config_file:
        mock_get_config_file.return_value = {"dbt-project-dir": "test_dbt_project"}
        with patch("dbt_checkpoint.utils.get_json") as mock_get_json:
            mock_get_json.return_value = {"key": "value"}
            expected_result = {"key": "value"}
            result = get_dbt_manifest(Args())
            assert result == expected_result


def test_get_dbt_catalog_with_config_project_dir():
    class Args:
        catalog = "target/catalog.json"
        config = ".dbt_checkpoint.yaml"

    with patch("dbt_checkpoint.utils.get_config_file") as mock_get_config_file:
        mock_get_config_file.return_value = {"dbt-project-dir": "test_dbt_project"}
        with patch("dbt_checkpoint.utils.get_json") as mock_get_json:
            mock_get_json.return_value = {"key": "value"}
            expected_result = {"key": "value"}
            result = get_dbt_catalog(Args())
            assert result == expected_result


@pytest.mark.parametrize(
    argnames=(
        "filepath",
        "project_dir_path",
        "expected_relative_path",
    ),
    argvalues=(
        (
            Path("macros/with_description.sql"),
            None,
            Path("macros/with_description.sql"),
        ),
        (
            Path("path/to/dbt/project/macros/with_description.sql"),
            "path/to/dbt/project",
            Path("macros/with_description.sql"),
        ),
        (
            Path("non-dbt-project-file.yaml"),
            "path/to/dbt/project",
            Path("non-dbt-project-file.yaml"),
        ),
    ),
)
def test_get_path_relative_to_dbt_project_dir(
    filepath, project_dir_path, expected_relative_path
):
    actual = get_path_relative_to_dbt_project_dir(
        filepath,
        project_dir_path,
    )
    assert actual == expected_relative_path


@pytest.mark.parametrize(
    argnames=("macro_id", "macro_sql", "expected_args"),
    argvalues=(
        ("test_macro", "{% macro test_macro() %}...{% endmacro %}", set()),
        (
            "test_macro",
            "{% macro test_macro(test_arg) %}...{% endmacro %}",
            {"test_arg"},
        ),
        (
            "package.test_macro",
            "{% macro test_macro(test_arg) %}...{% endmacro %}",
            {"test_arg"},
        ),
        (
            "test_macro",
            "{% macro test_macro(test_arg1, test_arg2) %}...{% endmacro %}",
            {"test_arg1", "test_arg2"},
        ),
        (
            "test_macro",
            "{% macro test_macro(test_arg) %}...{% endmacro %}..."
            "{% macro another_macro(test_arg) %}...{% endmacro %}",
            {"test_arg"},
        ),
    ),
)
def test_get_macro_args_from_sql_code(macro_id, macro_sql, expected_args):
    macro = MagicMock()
    macro.macro_sql = macro_sql
    macro.macro_id = macro_id
    assert get_macro_args_from_sql_code(macro) == expected_args


@pytest.mark.parametrize(
    argnames=("template_text", "expected_name", "expected_args"),
    argvalues=(
        (
            "{% test test_name(test_arg_1, test_arg_2) %}test body{% endtest %}",
            "test_test_name",
            ["test_arg_1", "test_arg_2"],
        ),
        (
            "{% test another_test_name() %}test body{% endtest %}",
            "test_another_test_name",
            [],
        ),
        (
            "{% macro test_macro() %}test body{% endmacro %}",
            "test_macro",
            [],
        ),
    ),
)
def test_test_jinja2_macro_extension_parse(template_text, expected_name, expected_args):
    template_text = template_text
    env = Environment()
    env.add_extension(Jinja2TestMacroExtension)
    result = env.parse(template_text)
    test = result.body[0]
    assert getattr(test, "name") == expected_name
    assert [arg.name for arg in getattr(test, "args", [])] == expected_args
