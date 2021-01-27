import pytest

from pre_commit_dbt.check_model_has_tests_by_name import main


# Input schema, input_args, valid_manifest, expected return value
# Input args, valid manifest, expected return value
TESTS = (
    (["aa/bb/with_test1.sql"], ["--tests", "unique=1", "data=1"], True, 0),
    (["aa/bb/with_test1.sql"], ["--tests", "unique=1"], True, 0),
    (["aa/bb/with_test1.sql"], ["--tests", "unique=2"], True, 1),
    (["aa/bb/with_test1.sql"], ["--tests", "data=1"], True, 0),
    (["aa/bb/with_test2.sql"], ["--tests", "data=1"], True, 1),
    (["aa/bb/with_test2.sql"], ["--tests", "unique=1"], True, 0),
    (["aa/bb/with_test1.sql"], ["--tests", "unique=1", "data=1"], False, 1),
)

ERROR_TESTS = ((["aa/bb/with_test1.sql"], ["--tests", "unique=1", "data=foo"]),)


@pytest.mark.parametrize(
    ("input_schema", "input_args", "valid_manifest", "expected_status_code"), TESTS
)
def test_check_model_has_tests_by_name(
    input_schema, input_args, valid_manifest, expected_status_code, manifest_path_str
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])
    input_schema.extend(input_args)
    status_code = main(input_schema)
    assert status_code == expected_status_code


@pytest.mark.parametrize(("input_schema", "input_args"), ERROR_TESTS)
def test_check_model_has_tests_by_name_error(
    input_schema, input_args, manifest_path_str
):
    input_args.extend(["--manifest", manifest_path_str])
    input_schema.extend(input_args)
    with pytest.raises(SystemExit):
        main(input_schema)
