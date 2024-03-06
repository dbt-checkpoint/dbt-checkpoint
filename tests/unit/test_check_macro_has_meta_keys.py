from unittest.mock import MagicMock, patch

import pytest

from dbt_checkpoint.check_macro_has_meta_keys import has_meta_key, main


@pytest.fixture
def mock_macro():
    macro = MagicMock()
    macro.name = "test_macro"
    macro.schema = {"meta": {"key1": "value1", "key2": "value2"}}
    return macro


def test_has_meta_key_match(mock_macro):
    paths = ["path/to/macros"]
    manifest = {"macros": [mock_macro]}
    meta_keys = ["key1", "key2"]
    allow_extra_keys = False

    with patch(
        "dbt_checkpoint.check_macro_has_meta_keys.get_filenames",
        return_value=["macro.sql"],
    ), patch(
        "dbt_checkpoint.check_macro_has_meta_keys.get_macros", return_value=[mock_macro]
    ):
        result = has_meta_key(paths, manifest, meta_keys, allow_extra_keys)

    assert result["status_code"] == 0


def test_has_meta_key_extra_keys(mock_macro):
    paths = ["path/to/macros"]
    manifest = {"macros": [mock_macro]}
    meta_keys = ["key1"]
    allow_extra_keys = False

    with patch(
        "dbt_checkpoint.check_macro_has_meta_keys.get_filenames",
        return_value=["macro.sql"],
    ), patch(
        "dbt_checkpoint.check_macro_has_meta_keys.get_macros", return_value=[mock_macro]
    ):
        result = has_meta_key(paths, manifest, meta_keys, allow_extra_keys)

    assert result["status_code"] == 1


def test_has_meta_key_allow_extra_keys(mock_macro):
    paths = ["path/to/macros"]
    manifest = {"macros": [mock_macro]}
    meta_keys = ["key1"]
    allow_extra_keys = True

    with patch(
        "dbt_checkpoint.check_macro_has_meta_keys.get_filenames",
        return_value=["macro.sql"],
    ), patch(
        "dbt_checkpoint.check_macro_has_meta_keys.get_macros", return_value=[mock_macro]
    ):
        result = has_meta_key(paths, manifest, meta_keys, allow_extra_keys)

    assert result["status_code"] == 0


if __name__ == "__main__":
    pytest.main()
