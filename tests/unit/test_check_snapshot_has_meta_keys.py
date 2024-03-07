from unittest.mock import MagicMock, patch

import pytest

from dbt_checkpoint.check_snapshot_has_meta_keys import has_meta_key, main


@pytest.fixture
def mock_snapshot():
    snapshot = MagicMock()
    snapshot.name = "test_snapshot"
    snapshot.schema = {"meta": {"key1": "value1", "key2": "value2"}}
    return snapshot


def test_has_meta_key_match(mock_snapshot):
    paths = ["path/to/snapshots"]
    manifest = {"snapshots": [mock_snapshot]}
    meta_keys = ["key1", "key2"]
    allow_extra_keys = False

    with patch(
        "dbt_checkpoint.check_snapshot_has_meta_keys.get_filenames",
        return_value={"snapshot.yml": "path/to/snapshots/snapshot.yml"},
    ), patch(
        "dbt_checkpoint.check_snapshot_has_meta_keys.get_snapshots",
        return_value=[mock_snapshot],
    ):
        result = has_meta_key(paths, manifest, meta_keys, allow_extra_keys)

    assert result["status_code"] == 0


def test_has_meta_key_extra_keys(mock_snapshot):
    paths = ["path/to/snapshots"]
    manifest = {"snapshots": [mock_snapshot]}
    meta_keys = ["key1"]
    allow_extra_keys = False

    with patch(
        "dbt_checkpoint.check_snapshot_has_meta_keys.get_filenames",
        return_value={"snapshot.yml": "path/to/snapshots/snapshot.yml"},
    ), patch(
        "dbt_checkpoint.check_snapshot_has_meta_keys.get_snapshots",
        return_value=[mock_snapshot],
    ):
        result = has_meta_key(paths, manifest, meta_keys, allow_extra_keys)

    assert result["status_code"] == 1


def test_has_meta_key_allow_extra_keys(mock_snapshot):
    paths = ["path/to/snapshots"]
    manifest = {"snapshots": [mock_snapshot]}
    meta_keys = ["key1"]
    allow_extra_keys = True

    with patch(
        "dbt_checkpoint.check_snapshot_has_meta_keys.get_filenames",
        return_value={"snapshot.yml": "path/to/snapshots/snapshot.yml"},
    ), patch(
        "dbt_checkpoint.check_snapshot_has_meta_keys.get_snapshots",
        return_value=[mock_snapshot],
    ):
        result = has_meta_key(paths, manifest, meta_keys, allow_extra_keys)

    assert result["status_code"] == 0


if __name__ == "__main__":
    pytest.main()
