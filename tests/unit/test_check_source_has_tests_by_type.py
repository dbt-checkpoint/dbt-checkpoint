import pytest

from pre_commit_dbt.check_source_has_tests_by_type import main


# Input schema, input_args, valid_manifest, expected return value
TESTS = (
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        ["--tests", "schema=1", "data=1", "--is_test"],
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        ["--tests", "schema=1", "data=1", "--is_test"],
        False,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        ["--tests", "schema=1", "--is_test"],
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        ["--tests", "schema=2", "--is_test"],
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        ["--tests", "schema=1", "data=1", "--is_test"],
        True,
        False,
        0,
    ),
)

ERROR_TESTS = (
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        ["--tests", "schma=1", "data=1", "--is_test"],
        True,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test1
        description: test description
    """,
        ["--tests", "schema=1", "data=foo", "--is_test"],
        True,
    ),
)


@pytest.mark.parametrize(
    (
        "input_schema",
        "input_args",
        "valid_manifest",
        "valid_config",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_source_has_tests_by_type(
    input_schema,
    input_args,
    valid_manifest,
    valid_config,
    expected_status_code,
    manifest_path_str,
    config_path_str,
    tmpdir,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), *input_args])
    assert status_code == expected_status_code


@pytest.mark.parametrize(("input_schema", "input_args", "valid_manifest"), ERROR_TESTS)
def test_check_source_has_tests_by_type_error(
    input_schema, input_args, valid_manifest, manifest_path_str, tmpdir
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    with pytest.raises(SystemExit):
        main(argv=[str(yml_file), *input_args])
