import pytest

from dbt_checkpoint.check_model_has_constraints import main


CONSTRAINTS = ('[{"type":"primary_key","columns":["col_a","col_b"]},'
               '{"type":"foreign_key","columns":["col_a","col_b"]},'
               '{"type":"check","columns":["col_a","col_b"]},'
               '{"type":"not_null","columns":["col_a","col_b"]},'
               '{"type":"unique","columns":["col_a","col_b"]},{"type":"custom"}]')

PRIMARY_KEY = '[{"type":"primary_key","columns":["col_a","col_b"]}]'

TESTS = (  # type: ignore
    (["aa/bb/with_no_constraints.sql", "--constraints", CONSTRAINTS], True, 1),
    (["aa/bb/with_empty_constraints.sql", "--constraints", CONSTRAINTS], True, 1),
    (["aa/bb/with_constraints.sql", "--constraints", CONSTRAINTS], True, 0),
    (["aa/bb/with_constraints_no_columns.sql", "--constraints", PRIMARY_KEY], True, 1),
    (["aa/bb/with_constraints_no_match.sql", "--constraints", CONSTRAINTS], True, 1),
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
