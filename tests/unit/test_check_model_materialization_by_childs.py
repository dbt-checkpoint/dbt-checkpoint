"""Unit testing the check_model_materialization_by_childs function."""
import pytest
import subprocess
import sys
from unittest.mock import patch

from dbt_checkpoint.check_model_materialization_by_childs import main
from dbt_checkpoint.utils import JsonOpenError


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


def test_check_model_materialization_manifest_error():
    """Test that the script exits with 1 when the manifest can't be loaded."""
    with patch(
        "dbt_checkpoint.check_model_materialization_by_childs.get_dbt_manifest"
    ) as mock_get_manifest:
        mock_get_manifest.side_effect = JsonOpenError("Mocked error")
        return_code = main(["some/sql/file.sql"])
        assert return_code == 1


def test_check_model_materialization_as_script(tmp_path):
    """Test the script's entry point when run from the command line."""
    # Create a dummy sql file to pass to the script
    sql_file = tmp_path / "model.sql"
    sql_file.write_text("select 1")

    script_path = "dbt_checkpoint/check_model_materialization_by_childs.py"

    # We expect this to fail because the manifest.json won't be found
    process = subprocess.run(
        [
            sys.executable,
            script_path,
            str(sql_file),
        ],
        capture_output=True,
        text=True,
    )
    assert process.returncode == 1
    assert "Unable to load manifest file" in process.stdout