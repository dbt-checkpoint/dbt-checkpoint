"""Unit testing the check_model_materialization_by_childs function."""
import pytest

from dbt_checkpoint.check_model_materialization_by_childs import main


TEST_INPUT = (
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--threshold-childs", "1"],
        True,
        True,
        1,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--threshold-childs", "2"],
        True,
        True,
        0,
    ),
    (["aa/bb/with_test1.sql", "--is_test"], ["--threshold-childs", "0"], True, True, 0),
    (["aa/bb/with_test1.sql", "--is_test"], ["--threshold-childs", "1"], True, True, 1),
    (
        ["aa/bb/without_test.sql", "--is_test"],
        ["--threshold-childs", "0"],
        True,
        True,
        1,
    ),
)


@pytest.mark.parametrize(
    (
        "input_model",
        "input_args",
        "valid_manifest",
        "valid_config",
        "expected_return_code",
    ),
    TEST_INPUT,
)
def test_check_model_materialization_by_childs(
    input_model,
    input_args,
    valid_manifest,
    valid_config,
    expected_return_code,
    manifest_path_str,
    config_path_str,
) -> None:
    """Test the check_model_materialization_by_childs function."""
    if valid_manifest:
        input_args.extend(["--manifest", manifest_path_str])

    if valid_config:
        input_args.extend(["--config", config_path_str])

    input_model.extend(input_args)
    return_code = main(input_model)
    assert return_code == expected_return_code
