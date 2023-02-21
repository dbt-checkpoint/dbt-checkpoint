import pytest

from pre_commit_dbt.check_source_columns_have_desc import main


# Input schema, expected return value
TESTS = (
    (
        """
sources:
-   name: test
    tables:
    -   name: test
        columns:
           - name: col1
             description: test
           - name: col2
             description: test
    """,
        True,
        0,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test
        columns:
           - name: col1
           - name: col2
             description: test
    """,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test
        columns:
           - name: col1
    """,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test
        columns:
           - name: col1
             description: test
           - name: col2
             description: test
    -   name: test2
        columns:
           - name: col1
           - name: col2
             description: test
    """,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test
    """,
        True,
        0,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: test
        columns:
           - name: col1
             description: test
           - name: col2
             description: test
    """,
        False,
        0,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "valid_config", "expected_status_code"), TESTS
)
def test_check_source_columns_have_desc(
    input_schema,
    valid_config,
    expected_status_code,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    input_args = ["--manifest", manifest_path_str, "--is_test"]
    if valid_config:
        input_args.extend(["--config", config_path_str])

    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), *input_args])
    assert status_code == expected_status_code
