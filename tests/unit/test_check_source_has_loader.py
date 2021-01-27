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
        0,
    ),
    (
        """
sources:
-   name: without_loader
    tables:
    -   name: test
    """,
        1,
    ),
)


@pytest.mark.parametrize(("input_schema", "expected_status_code"), TESTS)
def test_check_source_has_loader(input_schema, expected_status_code, tmpdir):
    yml_file = tmpdir.join("schema.yml")
    yml_file.write(input_schema)
    status_code = main(argv=[str(yml_file)])
    assert status_code == expected_status_code
