import pytest

from pre_commit_dbt.check_source_has_freshness import main


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
    tables:
    -   name: with_description
    """,
        0,
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
        1,
    ),
)


@pytest.mark.parametrize(("input_schema", "expected_status_code"), TESTS)
def test_check_source_has_freshness(input_schema, expected_status_code, tmpdir):
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file), "--freshness", "error_after", "warn_after"])
    assert status_code == expected_status_code
