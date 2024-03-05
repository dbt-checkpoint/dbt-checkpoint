from unittest.mock import MagicMock, patch

import pytest

from dbt_checkpoint.check_test_has_meta_keys import has_meta_key, main


@pytest.fixture
def mock_test():
    test = MagicMock()
    test.name = "test_test"
    test.schema = {"meta": {"key1": "value1", "key2": "value2"}}
    return test


def test_has_meta_key_match(mock_test):
    paths = ["path/to/tests"]
    manifest = {"tests": [mock_test]}
    meta_keys = ["key1", "key2"]
    allow_extra_keys = False

    with patch(
        "dbt_checkpoint.check_test_has_meta_keys.get_filenames",
        return_value={"test.sql": "path/to/tests/test.sql"},
    ), patch(
        "dbt_checkpoint.check_test_has_meta_keys.get_tests", return_value=[mock_test]
    ):
        result = has_meta_key(paths, manifest, meta_keys, allow_extra_keys)

    assert result["status_code"] == 0


def test_has_meta_key_extra_keys(mock_test):
    paths = ["path/to/tests"]
    manifest = {"tests": [mock_test]}
    meta_keys = ["key1"]
    allow_extra_keys = False

    with patch(
        "dbt_checkpoint.check_test_has_meta_keys.get_filenames",
        return_value={"test.sql": "path/to/tests/test.sql"},
    ), patch(
        "dbt_checkpoint.check_test_has_meta_keys.get_tests", return_value=[mock_test]
    ):
        result = has_meta_key(paths, manifest, meta_keys, allow_extra_keys)

    assert result["status_code"] == 1


def test_has_meta_key_allow_extra_keys(mock_test):
    paths = ["path/to/tests"]
    manifest = {"tests": [mock_test]}
    meta_keys = ["key1"]
    allow_extra_keys = True

    with patch(
        "dbt_checkpoint.check_test_has_meta_keys.get_filenames",
        return_value={"test.sql": "path/to/tests/test.sql"},
    ), patch(
        "dbt_checkpoint.check_test_has_meta_keys.get_tests", return_value=[mock_test]
    ):
        result = has_meta_key(paths, manifest, meta_keys, allow_extra_keys)

    assert result["status_code"] == 0


if __name__ == "__main__":
    pytest.main()
