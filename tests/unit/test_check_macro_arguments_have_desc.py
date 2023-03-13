import pytest

from pre_commit_dbt.check_macro_arguments_have_desc import check_argument_desc
from pre_commit_dbt.check_macro_arguments_have_desc import main


# Input args, valid manifest, expected return value
TESTS = (
    (["macros/aa/with_argument_description.sql", "--is_test"], True, True, 0),
    (["macros/aa/with_argument_description.sql", "--is_test"], False, True, 1),
    (["macros/aa/without_arguments_description.sql", "--is_test"], True, True, 1),
    (["macros/aa/with_some_argument_description.sql", "--is_test"], True, True, 1),
    (["macros/aa/with_argument_description.sql", "--is_test"], True, False, 0),
)


@pytest.mark.parametrize(
    ("input_args", "valid_manifest", "valid_config", "expected_status_code"), TESTS
)
def test_check_macro_arguments_have_desc(
    input_args,
    valid_manifest,
    valid_config,
    expected_status_code,
    manifest_path_str,
    config_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    status_code = main(input_args)
    assert status_code == expected_status_code


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_macro_arguments_have_desc_in_schema(
    extension, tmpdir, manifest_path_str
):
    schema_yml = """
version: 2
macros:
-   name: in_schema_argument_description
    arguments:
    -   name: test
        description: aaa
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "in_schema_argument_description.sql",
            str(yml_file),
            "--is_test",
            "--manifest",
            manifest_path_str,
        ],
    )
    assert result == 0


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_argument_desc(extension, tmpdir, manifest):
    schema_yml = """
version: 2
macros:
-   name: with_some_argument_description
    arguments:
    -   name: test1
        description: aaa
    -   name: test2
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    hook_properties = check_argument_desc(
        ["macros/aa/with_some_argument_description.sql", str(yml_file), "--is_test"],
        manifest,
    )
    res_stat = hook_properties["status_code"]
    missing = hook_properties["missing"]
    assert res_stat == 1
    assert missing == {"with_some_argument_description": {"test2"}}


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_macro_arguments_have_desc_both(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2
macros:
-   name: with_some_argument_description
    arguments:
    -   name: test4
-   name: without_arguments_description
    arguments:
    -   name: test
-   name: xxx
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "macros/aa/with_some_argument_description.sql",
            "macros/aa/without_arguments_description.sql",
            str(yml_file),
            "--manifest",
            manifest_path_str,
            "--is_test",
        ],
    )
    assert result == 1


@pytest.mark.parametrize("extension", [("yml"), ("yaml")])
def test_check_macro_arguments_have_desc_without(extension, tmpdir, manifest_path_str):
    schema_yml = """
version: 2
macros:
-   name: without_arguments_description
    arguments:
    -   name: test1
        description: aa
    -   name: test2
        description: bb
    """
    yml_file = tmpdir.join(f"schema.{extension}")
    yml_file.write(schema_yml)
    result = main(
        argv=[
            "macros/aa/without_arguments_description.sql",
            str(yml_file),
            "--manifest",
            manifest_path_str,
            "--is_test",
        ],
    )
    assert result == 0
