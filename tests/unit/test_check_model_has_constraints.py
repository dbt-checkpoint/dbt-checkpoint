import pytest

from dbt_checkpoint.check_model_has_constraints import main

# Input schema, input_args, valid_manifest, valid_config, expected return value
# Input args, valid manifest, expected return value
TESTS = (  # type: ignore
    (["aa/bb/with_test1.sql", "--is_test"], ["--test-cnt", "1"], True, True, 0),
    (["aa/bb/with_test1.sql", "--is_test"], ["--test-cnt", "2"], True, True, 0),
    (["aa/bb/with_test1.sql", "--is_test"], [], True, True, 0),
    (["aa/bb/with_test1.sql", "--is_test"], ["--test-cnt", "3"], True, True, 1),
    (["aa/bb/without_test.sql", "--is_test"], ["--test-cnt", "1"], True, True, 1),
    (["aa/bb/with_test1.sql", "--is_test"], ["--test-cnt", "1"], False, True, 1),
    (["aa/bb/with_test1.sql", "--is_test"], ["--test-cnt", "1"], True, False, 0),
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
def test_check_model_has_tests(
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
