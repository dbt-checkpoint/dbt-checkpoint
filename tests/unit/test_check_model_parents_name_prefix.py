import pytest
import subprocess
import sys
from unittest.mock import patch

from dbt_checkpoint.check_model_parents_name_prefix import main
from dbt_checkpoint.utils import JsonOpenError

# Input schema, input_args, valid_manifest, valid_config, expected return value
# Input args, valid manifest, expected return value
TESTS = (  # type: ignore
    (["aa/bb/parent_child.sql", "--is_test"], [], True, True, 1),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--whitelist", "ref", "without", "w"],
        True,
        True,
        0,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--whitelist", "ref"],
        True,
        True,
        1,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--whitelist", "somthing_else"],
        True,
        True,
        1,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--whitelist", "something", "ref", "without"],
        True,
        True,
        0,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--blacklist", "dev", "dev1"],
        True,
        True,
        0,
    ),
    (
        ["aa/bb/parent_child.sql", "--is_test"],
        ["--blacklist", "without_tags", "source2", "dev1"],
        True,
        True,
        1,
    ),
    (["aa/bb/parent_child.sql", "--is_test"], ["--blacklist", "dev"], True, True, 0),
    (["aa/bb/parent_child.sql", "--is_test"], ["--blacklist", "dev"], True, False, 0),
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
def test_check_model_parents_name_prefix(
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


def test_check_parents_prefix_manifest_error():
    """Test the script exits with 1 when the manifest can't be loaded."""
    with patch(
        "dbt_checkpoint.check_model_parents_name_prefix.get_dbt_manifest"
    ) as mock_get_manifest:
        mock_get_manifest.side_effect = JsonOpenError("Mocked error")
        return_code = main(["some/sql/file.sql", "--whitelist", "fct_"])
        assert return_code == 1


def test_check_parents_prefix_missing_list_arg(manifest_path_str):
    """Test the script exits with 1 if neither whitelist nor blacklist is provided."""
    # Note: We still need a manifest for the check to pass that stage
    return_code = main(["some/sql/file.sql", "--manifest", manifest_path_str])
    assert return_code == 1


def test_check_parents_prefix_as_script(tmp_path):
    """Test the script's entry point when run from the command line."""
    sql_file = tmp_path / "model.sql"
    sql_file.write_text("select 1")

    script_path = "dbt_checkpoint/check_model_parents_name_prefix.py"

    # We expect this to fail because we are not providing a manifest or a list
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