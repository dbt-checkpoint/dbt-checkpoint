import pytest
import subprocess
import sys
from unittest.mock import patch

from dbt_checkpoint.check_model_has_constraints import main
from dbt_checkpoint.utils import JsonOpenError


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
    # Test that a view without constraints passes (as it's skipped)
    (["aa/bb/view_with_no_constraints.sql", "--constraints", CONSTRAINTS], True, 0),
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


def test_check_model_has_constraints_manifest_error():
    """Test the script exits with 1 when the manifest can't be loaded."""
    with patch(
        "dbt_checkpoint.check_model_has_constraints.get_dbt_manifest"
    ) as mock_get_manifest:
        mock_get_manifest.side_effect = JsonOpenError("Mocked error")
        # A valid --constraints value is still required
        return_code = main(["some/sql/file.sql", "--constraints", "[]"])
        assert return_code == 1


def test_check_model_has_constraints_as_script(tmp_path):
    """Test the script's entry point when run from the command line."""
    sql_file = tmp_path / "model.sql"
    sql_file.write_text("select 1")

    script_path = "dbt_checkpoint/check_model_has_constraints.py"

    # We expect this to fail because we are not providing a manifest
    process = subprocess.run(
        [
            sys.executable,
            script_path,
            str(sql_file),
            "--constraints",
            "[]"
        ],
        capture_output=True,
        text=True,
    )
    assert process.returncode == 1
    assert "Unable to load manifest file" in process.stdout