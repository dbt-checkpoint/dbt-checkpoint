import pytest

from pre_commit_dbt.check_model_has_tests import main


# Input schema, input_args, valid_manifest, expected return value
# Input args, valid manifest, expected return value
TESTS = (  # type: ignore
    (["aa/bb/with_test1.sql"], ["--test-cnt", "1"], True, 0),
    (["aa/bb/with_test1.sql"], ["--test-cnt", "2"], True, 0),
    (["aa/bb/with_test1.sql"], [], True, 0),
    (["aa/bb/with_test1.sql"], ["--test-cnt", "3"], True, 1),
    (["aa/bb/without_test.sql"], ["--test-cnt", "1"], True, 1),
    (["aa/bb/with_test1.sql"], ["--test-cnt", "1"], False, 1),
)


@pytest.mark.parametrize(
    ("input_schema", "input_args", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_model_has_tests(
    input_schema, input_args, valid_manifest, expected_status_code, manifest_path_str
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    input_schema.extend(input_args)
    status_code = main(input_schema)
    assert status_code == expected_status_code
