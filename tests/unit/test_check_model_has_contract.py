import pytest

from dbt_checkpoint.check_model_has_contract import main

# Input schema, input_args, valid_manifest, valid_config, expected return value
# Input args, valid manifest, expected return value
TESTS = (  # type: ignore
    (["aa/bb/with_contract.sql"], True, 0),
    (["aa/bb/with_no_contract.sql"], True, 1),
)


@pytest.mark.parametrize(
    (
        "input_schema",
        "input_args",
        "valid_manifest",
        "expected_status_code",
    ),
    TESTS,
)
def test_check_model_has_contract(
    input_schema,
    input_args,
    valid_manifest,
    expected_status_code,
    manifest_path_str,
):
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    input_schema.extend(input_args)
    status_code = main(input_schema)
    assert status_code == expected_status_code
