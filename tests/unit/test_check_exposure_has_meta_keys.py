from unittest.mock import MagicMock, patch

import pytest

from dbt_checkpoint.check_exposure_has_meta_keys import has_meta_key, main


@pytest.fixture
def mock_object():
    obj = MagicMock()
    obj.name = "test_obj"
    obj.schema = {"meta": {"key1": "value1", "key2": "value2"}}
    return obj


def test_has_meta_key_match(mock_object):
    paths = ["path/to/exposure.yml"]
    meta_keys = ["key1", "key2"]
    allow_extra_keys = False

    with patch(
        "dbt_checkpoint.check_exposure_has_meta_keys.get_exposures",
        return_value=[mock_object],
    ):
        result = has_meta_key(paths, meta_keys, allow_extra_keys)

    assert result["status_code"] == 0


def test_has_meta_key_extra_keys(mock_object):
    paths = ["path/to/exposure.yml"]
    meta_keys = ["key1"]
    allow_extra_keys = False

    with patch(
        "dbt_checkpoint.check_exposure_has_meta_keys.get_exposures",
        return_value=[mock_object],
    ):
        result = has_meta_key(paths, meta_keys, allow_extra_keys)

    assert result["status_code"] == 1


def test_has_meta_key_allow_extra_keys(mock_object):
    paths = ["path/to/exposure.yml"]
    meta_keys = ["key1"]
    allow_extra_keys = True

    with patch(
        "dbt_checkpoint.check_exposure_has_meta_keys.get_exposures",
        return_value=[mock_object],
    ):
        result = has_meta_key(paths, meta_keys, allow_extra_keys)

    assert result["status_code"] == 0


if __name__ == "__main__":
    pytest.main()
