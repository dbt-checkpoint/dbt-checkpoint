import pytest

from pre_commit_dbt.check_model_has_tests_by_group import main


# Input schema, input_args, valid_manifest, valid_config, expected return value
# Input args, valid manifest, expected return value
TESTS = (
    (
        ["aa/bb/with_test1.sql", "--is_test"],
        ["--tests", "unique", "--test-cnt", "1"],
        True,
        True,
        0,
    ),
    (
        ["aa/bb/with_test3.sql", "--is_test"],
        ["--tests", "unique", "unique_where", "--test-cnt", "1"],
        True,
        True,
        0,
    ),
    (
        ["aa/bb/with_test3.sql", "--is_test"],
        ["--tests", "unique", "unique_where", "--test-cnt", "2"],
        True,
        True,
        0,
    ),
    (
        ["aa/bb/with_test1.sql", "--is_test"],
        ["--tests", "unique", "unique_where", "--test-cnt", "2"],
        True,
        True,
        1,
    ),
    (
        ["aa/bb/with_test1.sql", "--is_test"],
        ["--tests", "unique", "--test-cnt", "2"],
        True,
        True,
        1,
    ),
    (
        ["aa/bb/with_test3.sql", "--is_test"],
        ["--tests", "unique", "--test-cnt", "1"],
        False,
        True,
        1,
    ),
    (
        ["aa/bb/with_test1.sql", "--is_test"],
        ["--tests", "unique", "--test-cnt", "1"],
        True,
        False,
        0,
    ),
)

ERROR_TESTS = (
    (
        ["aa/bb/with_test1.sql"],
        ["--tests", "unique", "unique_where", "--test-cnt", "foo"],
    ),
)


@pytest.mark.parametrize(
    (
        "input_schema",
        "input_args",
        "valid_manifest",
        "valid_config",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_model_has_tests_by_group(
    input_schema,
    input_args,
    valid_manifest,
    valid_config,
    expected_status_code,
    manifest_path_str,
    config_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    input_schema.extend(input_args)
    status_code = main(input_schema)
    assert status_code == expected_status_code


@pytest.mark.parametrize(("input_schema", "input_args"), ERROR_TESTS)
def test_check_model_has_tests_by_group_error(
    input_schema, input_args, manifest_path_str
):
    input_args.extend(["--manifest", manifest_path_str])
    input_schema.extend(input_args)
    with pytest.raises(SystemExit):
        main(input_schema)
