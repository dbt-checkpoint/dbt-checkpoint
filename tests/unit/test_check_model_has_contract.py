from pathlib import Path
from unittest.mock import patch

import pytest

from dbt_checkpoint.check_model_has_contract import main
from dbt_checkpoint.utils import JsonOpenError

# yapf: disable
MANIFEST_FILE_WITHOUT_CONTRACT = {
    "nodes": {
        "model.package.model_without_contract": {
            "resource_type": "model",
            "name": "model_without_contract",
            "config": {},
            "original_file_path": str(Path("models/model_without_contract.sql")),
        }
    }
}

MANIFEST_FILE_WITH_CONTRACT = {
    "nodes": {
        "model.package.model_with_contract": {
            "resource_type": "model",
            "name": "model_with_contract",
            "config": {"contract": {"enforced": True}},
            "original_file_path": str(Path("models/model_with_contract.sql")),
        }
    }
}

MANIFEST_FILE_DISABLED_MODEL = {
    "nodes": {
        "model.package.disabled_model": {
            "resource_type": "model",
            "name": "disabled_model",
            "config": {"enabled": False},
            "original_file_path": str(Path("models/disabled_model.sql")),
        }
    }
}
# yapf: enable


@pytest.mark.parametrize(
    ("manifest", "files", "include_disabled", "exclude", "expected_code"),
    [
        # Happy path: model has a contract
        (MANIFEST_FILE_WITH_CONTRACT, ["models/model_with_contract.sql"], False, "", 0),
        # Failure path: model is missing a contract
        (
            MANIFEST_FILE_WITHOUT_CONTRACT,
            ["models/model_without_contract.sql"],
            False,
            "",
            1,
        ),
        # Edge case: two models, one missing a contract, but it's excluded
        (
            MANIFEST_FILE_WITHOUT_CONTRACT,
            ["models/model_without_contract.sql"],
            False,
            r"model_without_contract\.sql", # Regex to exclude the model
            0,
        ),
        # Edge case: a disabled model is missing a contract but is not included
        (
            MANIFEST_FILE_DISABLED_MODEL,
            ["models/disabled_model.sql"],
            False,
            "",
            0,
        ),
        # Edge case: a disabled model is missing a contract and IS included
        (
            MANIFEST_FILE_DISABLED_MODEL,
            ["models/disabled_model.sql"],
            True,
            "",
            1,
        ),
    ],
)
def test_check_model_has_contract(
    manifest, files, include_disabled, exclude, expected_code, tmp_path
):
    """
    Tests the check_model_has_contract function with various scenarios.
    """
    for file in files:
        (tmp_path / file).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / file).write_text("select 1")

    args = files
    if include_disabled:
        args.append("--include-disabled")
    if exclude:
        args.extend(["--exclude", exclude])

    with patch(
        "dbt_checkpoint.check_model_has_contract.get_dbt_manifest",
        return_value=manifest,
    ):
        # cd into the temp directory to simulate running from the project root
        with patch("os.chdir", return_value=tmp_path):
            status_code = main(args)
            assert status_code == expected_code


@patch("dbt_checkpoint.check_model_has_contract.get_dbt_manifest")
def test_check_model_has_contract_manifest_error(mock_get_manifest):
    """
    Tests that the script exits with 1 when the manifest can't be loaded.
    """
    mock_get_manifest.side_effect = JsonOpenError("Mocked error")
    status_code = main(["somet/path.sql"])
    assert status_code == 1