from unittest.mock import MagicMock, patch

import pytest

from dbt_checkpoint.check_exposure_has_meta_keys import has_meta_key, main


@pytest.fixture
def mock_exposure():
    exposure = MagicMock()
    exposure.exposure_name = "test_exposure"
    exposure.exposure_schema = {"meta": {"key1": "value1", "key2": "value2"}}
    return exposure


def test_has_meta_key_match(mock_exposure):
    paths = ["path/to/exposure.yml"]
    meta_keys = ["key1", "key2"]
    allow_extra_keys = False

    with patch(
        "dbt_checkpoint.check_exposure_has_meta_keys.get_exposures",
        return_value=[mock_exposure],
    ):
        result = has_meta_key(paths, meta_keys, allow_extra_keys)

    assert result["status_code"] == 0


def test_has_meta_key_extra_keys(mock_exposure):
    paths = ["path/to/exposure.yml"]
    meta_keys = ["key1"]
    allow_extra_keys = False

    with patch(
        "dbt_checkpoint.check_exposure_has_meta_keys.get_exposures",
        return_value=[mock_exposure],
    ):
        result = has_meta_key(paths, meta_keys, allow_extra_keys)

    assert result["status_code"] == 1


def test_has_meta_key_allow_extra_keys(mock_exposure):
    paths = ["path/to/exposure.yml"]
    meta_keys = ["key1"]
    allow_extra_keys = True

    with patch(
        "dbt_checkpoint.check_exposure_has_meta_keys.get_exposures",
        return_value=[mock_exposure],
    ):
        result = has_meta_key(paths, meta_keys, allow_extra_keys)

    assert result["status_code"] == 0


if __name__ == "__main__":
    pytest.main()
