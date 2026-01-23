import pytest

from dbt_checkpoint.check_test_tags import main

TESTS = (
    #SQL file changed wich has more than one test. First test has tag "schema" and second test has a tag "data". 
    (["aa/bb/with_test1.sql", "--is_test", "--tags", "schema", "data"], True, True, 0),
    #SQL file changed wich has more than one test. First test has tag "schema" and second test has a tag "data". 
    (["aa/bb/with_test1.sql", "--is_test", "--tags", "data"], True, True, 1),
    #SQL file changed wich has more than one test. First test has tag "schema" and second test has a tag "data". 
    (["aa/bb/with_test1.sql", "--is_test", "--tags", "bar"], True, True, 1),
    
    
    # Model has only tag "schema" and no tag "bar"
    (["aa/bb/with_test2.sql", "--is_test", "--tags", "schema", "bar"], True, True, 0),
    # Model has tags but there is no tests added to the model.
    (["aa/bb/with_tags.sql", "--is_test", "--tags", "foo", "bar"], True, True, 0)
    
)


@pytest.mark.parametrize(
    ("input_args", "valid_manifest", "valid_config", "expected_status_code"), TESTS
)
def test_check_test_tags(
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

    status_code = main(input_args)
    assert status_code == expected_status_code

