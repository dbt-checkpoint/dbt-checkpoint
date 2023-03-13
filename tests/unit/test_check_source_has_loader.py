import pytest

from pre_commit_dbt.check_source_has_loader import main


# Input schema, expected return value
TESTS = (
    (
        """
sources:
-   name: with_loader
    loader: test
    tables:
    -   name: test
    """,
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: with_loader
    loader: test
    tables:
    -   name: test
    """,
        False,
        True,
        1,
    ),
    (
        """
sources:
-   name: without_loader
    tables:
    -   name: test
    """,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: with_loader
    loader: test
    tables:
    -   name: test
    """,
        True,
        False,
        0,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "valid_manifest", "valid_config", "expected_status_code"), TESTS
)
def test_check_source_has_loader(
    input_schema,
    valid_manifest,
    valid_config,
    expected_status_code,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    input_args = ["--is_test"]

    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), *input_args])
    assert status_code == expected_status_code
