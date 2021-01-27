import pytest

from pre_commit_dbt.check_source_table_has_description import main


# Input schema, expected return value
TESTS = (
    (
        """
sources:
-   name: test
    tables:
    -   name: with_description
        description: test description
    """,
        0,
    ),
    (
        """
sources:
-   name: test
    tables:
    -   name: without_description
    """,
        1,
    ),
)


@pytest.mark.parametrize(("input_schema", "expected_status_code"), TESTS)
def test_check_source_table_has_description(input_schema, expected_status_code, tmpdir):
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file)])
    assert status_code == expected_status_code
