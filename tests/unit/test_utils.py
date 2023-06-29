from pathlib import Path
from unittest.mock import patch

import pytest

from dbt_checkpoint.utils import (
    CalledProcessError,
    CompilationException,
    MacroSchema,
    Model,
    ModelSchema,
    SourceSchema,
    check_yml_version,
    cmd_output,
    get_filenames,
    get_macro_schemas,
    get_missing_file_paths,
    get_model_schemas,
    obj_in_deps,
    paths_to_dbt_models,
)


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


# Input files, valid manifest, expected files
TESTS = (
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
)


@pytest.mark.parametrize(
    (
        "input_files",
        "discovered_files",
        "valid_manifest",
        "exclude_regex",
        "expected_files",
    ),
    TESTS,
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
