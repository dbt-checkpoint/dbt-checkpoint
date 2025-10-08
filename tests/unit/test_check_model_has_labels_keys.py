import pytest
import subprocess
import sys
from unittest.mock import patch

from dbt_checkpoint.check_model_has_labels_keys import main
from dbt_checkpoint.utils import JsonOpenError

# yapf: disable
MANIFEST_FILE_WITH_LABELS = {
    "nodes": {
        "model.package.with_labels": {
            "resource_type": "model",
            "name": "with_labels",
            "config": {
                "labels": {"foo": "bar", "baz": "test"}
            },
            "original_file_path": "aa/bb/with_labels.sql",
        }
    }
}

MANIFEST_FILE_WITHOUT_LABELS = {
    "nodes": {
        "model.package.without_labels": {
            "resource_type": "model",
            "name": "without_labels",
            "config": {},
            "original_file_path": "aa/bb/without_labels.sql",
        }
    }
}

MANIFEST_FILE_WITHOUT_CONFIG = {
    "nodes": {
        "model.package.without_config": {
            "resource_type": "model",
            "name": "without_config",
            "original_file_path": "aa/bb/without_config.sql",
        }
    }
}

MANIFEST_FILE_DISABLED_MODEL = {
    "nodes": {
        "model.package.disabled_model_without_labels": {
            "resource_type": "model",
            "name": "disabled_model_without_labels",
            "config": {"enabled": False},
            "original_file_path": "aa/bb/disabled_model_without_labels.sql",
        }
    }
}

MANIFEST_FILE_INVALID_LABELS = {
    "nodes": {
        "model.package.invalid_labels": {
            "resource_type": "model",
            "name": "invalid_labels",
            "config": {
                "labels": "this-is-a-string"
            },
            "original_file_path": "aa/bb/invalid_labels.sql",
        }
    }
}
# yapf: enable


@pytest.mark.parametrize(
    ("files", "labels", "allow_extra", "manifest", "expected_code"),
    [
        # Happy path, exact match
        (["aa/bb/with_labels.sql"], ["foo", "baz"], False, MANIFEST_FILE_WITH_LABELS, 0),
        # Happy path, allow extra keys
        (["aa/bb/with_labels.sql"], ["foo"], True, MANIFEST_FILE_WITH_LABELS, 0),
        # Fail path, missing key
        (["aa/bb/with_labels.sql"], ["foo", "bar"], False, MANIFEST_FILE_WITH_LABELS, 1),
        # Fail path, extra key not allowed
        (["aa/bb/with_labels.sql"], ["foo"], False, MANIFEST_FILE_WITH_LABELS, 1),
        # Fail path, model is missing the 'labels' key
        (["aa/bb/without_labels.sql"], ["foo"], False, MANIFEST_FILE_WITHOUT_LABELS, 1),
        # Fail path, model is missing the 'config' key
        (["aa/bb/without_config.sql"], ["foo"], False, MANIFEST_FILE_WITHOUT_CONFIG, 1),
        # Fail path, labels is not a dictionary
        (["aa/bb/invalid_labels.sql"], ["foo"], False, MANIFEST_FILE_INVALID_LABELS, 1),
    ],
)
def test_check_model_has_labels_keys(
    files, labels, allow_extra, manifest, expected_code
):
    """Tests the check_model_has_labels_keys function with various scenarios."""
    args = files + ["--labels-keys"] + labels
    if not allow_extra:
        args.append("--no-allow-extra-keys")

    with patch(
        "dbt_checkpoint.check_model_has_labels_keys.get_dbt_manifest",
        return_value=manifest,
    ):
        status_code = main(args)
        assert status_code == expected_code


def test_check_model_has_labels_keys_disabled():
    """Tests the --include-disabled flag."""
    files = ["aa/bb/disabled_model_without_labels.sql"]
    args = files + ["--labels-keys", "foo"]

    # Check that the disabled model is ignored by default (status 0)
    with patch(
        "dbt_checkpoint.check_model_has_labels_keys.get_dbt_manifest",
        return_value=MANIFEST_FILE_DISABLED_MODEL,
    ):
        status_code = main(args)
        assert status_code == 0

    # Check that the disabled model is included and fails when the flag is passed (status 1)
    args_with_flag = args + ["--include-disabled"]
    with patch(
        "dbt_checkpoint.check_model_has_labels_keys.get_dbt_manifest",
        return_value=MANIFEST_FILE_DISABLED_MODEL,
    ):
        status_code = main(args_with_flag)
        assert status_code == 1


@patch("dbt_checkpoint.check_model_has_labels_keys.get_dbt_manifest")
def test_check_model_has_labels_keys_manifest_error(mock_get_manifest):
    """Tests that the script exits with 1 when the manifest can't be loaded."""
    mock_get_manifest.side_effect = JsonOpenError("Mocked error")
    status_code = main(["aa/bb/with_labels.sql", "--labels-keys", "foo"])
    assert status_code == 1


def test_check_model_has_labels_keys_as_script(tmp_path):
    """Test the script's entry point when run from the command line."""
    sql_file = tmp_path / "model.sql"
    sql_file.write_text("select 1")

    script_path = "dbt_checkpoint/check_model_has_labels_keys.py"

    # We expect this to fail because we are not providing a manifest file
    process = subprocess.run(
        [
            sys.executable,
            script_path,
            str(sql_file),
            "--labels-keys",
            "foo"
        ],
        capture_output=True,
        text=True,
    )
    assert process.returncode == 1
    assert "Unable to load manifest file" in process.stdout