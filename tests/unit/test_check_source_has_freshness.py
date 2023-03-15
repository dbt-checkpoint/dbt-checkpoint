import pytest

from dbt_checkpoint.check_source_has_freshness import main

# Input schema, expected return value
TESTS = (
    (
        """
sources:
-   name: test
    loaded_at_field: aa
    freshness:
        warn_after:
            count: 12
            period: hour
        error_after:
            count: 24
            period: hour
        filter: x > y
    tables:
    -   name: with_description
    """,
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: test
    loaded_at_field: aa
    freshness:
        warn_after:
            count: 12
            period: hour
        error_after:
            count: 24
            period: hour
    tables:
    -   name: with_description
    """,
        False,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    freshness:
        warn_after:
            count: 12
            period: hour
        error_after:
            count: 24
            period: hour
    tables:
    -   name: with_description
    """,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    loaded_at_field: aa
    freshness:
        warn_after:
            count: 12
            period: hour
    tables:
    -   name: with_description
    """,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    loaded_at_field: aa
    tables:
    -   name: with_description
    """,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: with_description
        loaded_at_field: aa
        freshness:
            warn_after:
                count: 12
                period: hour
            error_after:
                count: 24
                period: hour
    """,
        True,
        True,
        0,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: with_description
        freshness:
            warn_after:
                count: 12
                period: hour
            error_after:
                count: 24
                period: hour
    """,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: with_description
        loaded_at_field: aa
        freshness:
            warn_after:
                count: 12
                period: hour
    """,
        True,
        True,
        1,
    ),
    (
        """
sources:
-   name: test
    loaded_at_field: aa
    freshness:
        warn_after:
            count: 12
            period: hour
        error_after:
            count: 24
            period: hour
    tables:
    -   name: with_description
    """,
        True,
        False,
        0,
    ),
)


@pytest.mark.parametrize(
    ("input_schema", "valid_manifest", "valid_config", "expected_status_code"), TESTS
)
def test_check_source_has_freshness(
    input_schema,
    valid_manifest,
    valid_config,
    expected_status_code,
    tmpdir,
    manifest_path_str,
    config_path_str,
):
    input_args = [
        "--freshness",
        "error_after",
        "warn_after",
        "--is_test",
    ]

    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), *input_args])
    assert status_code == expected_status_code
