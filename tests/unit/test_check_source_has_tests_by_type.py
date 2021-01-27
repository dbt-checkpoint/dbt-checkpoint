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
        ["--tests", "schema=1", "data=1"],
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
        ["--tests", "schema=1", "data=1"],
        False,
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
        ["--tests", "schema=1"],
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
        ["--tests", "schema=2"],
        True,
        1,
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
        ["--tests", "schma=1", "data=1"],
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
        ["--tests", "schema=1", "data=foo"],
        True,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "input_args", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_source_has_tests_by_type(
    input_schema,
    input_args,
    valid_manifest,
    expected_status_code,
    manifest_path_str,
    tmpdir,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
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
