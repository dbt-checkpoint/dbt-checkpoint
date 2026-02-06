import pytest

from dbt_checkpoint.check_model_has_generic_constraints import main


CONSTRAINTS = ["primary_key", "foreign_key", "check", "not_null", "unique"]
# PRIMARY_KEY = '[{"type":"primary_key","columns":["col_a","col_b"]}]'

TESTS = (  # type: ignore
    (["aa/bb/with_no_constraints.sql", "--constraints", *CONSTRAINTS], True, 1),
    (["aa/bb/with_constraints.sql", "--constraints", *CONSTRAINTS], True, 0),
    (["aa/bb/with_constraints.sql", "--constraints", "custom_constraint"], True, 1),
    (
        ["aa/bb/with_constraints_no_columns.sql", "--constraints", "primary_key"],
        True,
        0,
    ),
    (["aa/bb/with_constraints_no_match.sql", "--constraints", *CONSTRAINTS], True, 1),
)


@pytest.mark.parametrize(
    (
        "input_args",
        "valid_manifest",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_model_has_constraints(
    input_args,
    valid_manifest,
    expected_status_code,
    manifest_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    status_code = main(input_args)
    assert status_code == expected_status_code
