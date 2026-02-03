from unittest.mock import patch

import pytest

from dbt_checkpoint.check_script_ref_and_source import check_refs_sources, main
from dbt_checkpoint.utils import JsonOpenError

# Input, expected return value, expected output
# Test for the happy path where all dependencies exist
TESTS_MANIFEST_REFS = (
    (
        0,
        {
            "nodes": {
                "test_model": {
                    "original_file_path": "model/test_model.sql",
                    "depends_on": {
                        "nodes": [
                            "model.package1.ref1",
                            "model.package2.ref2",
                            "model.package3.ref3.v1",
                        ]
                    },
                },
                "model.package1.ref1": {
                    "name": "ref1",
                    "unique_id": "model.package1.ref1",
                },
                "model.package2.ref2": {
                    "name": "ref2",
                    "unique_id": "model.package2.ref2",
                    "original_file_path": "model/ref2.sql",
                },
                "model.package3.ref3.v1": {
                    "name": "ref3",
                    "unique_id": "model.package3.ref3.v1",
                },
            }
        },
    ),
)


@pytest.mark.parametrize(
    ("expected_status_code", "manifest"),
    TESTS_MANIFEST_REFS,
)
def test_check_script_ref_and_source_manifest_refs(
    expected_status_code,
    manifest,
    tmpdir,
):
    path = tmpdir.join("file.sql")
    path.write("select 1")
    manifest["nodes"]["test_model"]["original_file_path"] = str(path)
    hook_properties = check_refs_sources(paths=[path], manifest=manifest)
    ret = hook_properties.get("status_code")
    assert ret == expected_status_code


# Tests for missing refs/sources
TESTS_MANIEST_MISSING = (
    # Missing model ref
    (
        1,
        "model/test_model.sql",
        {
            "nodes": {
                "test_model": {
                    "depends_on": {"nodes": ["model.package.missing_ref"]},
                }
            }
        },
    ),
    # Missing source
    (
        1,
        "model/test_model.sql",
        {
            "nodes": {
                "test_model": {
                    "depends_on": {"nodes": ["source.package.missing_source.table"]},
                }
            },
            "sources": {},  # Sources dictionary is empty
        },
    ),
)


@pytest.mark.parametrize(
    ("expected_status_code", "path_str", "manifest"),
    TESTS_MANIEST_MISSING,
)
def test_check_script_ref_and_source_manifest_missing(
    expected_status_code,
    path_str,
    manifest,
    tmpdir,
):
    path = tmpdir.join(path_str)
    # Add ensure=True to create the parent 'model' directory
    path.write("select 1", ensure=True)
    manifest["nodes"]["test_model"]["original_file_path"] = str(path)
    hook_properties = check_refs_sources(paths=[path], manifest=manifest)
    ret = hook_properties.get("status_code")
    assert ret == expected_status_code


# Tests for the main function
@patch("dbt_checkpoint.check_script_ref_and_source.get_dbt_manifest")
@patch("dbt_checkpoint.check_script_ref_and_source.dbtCheckpointTracking")
def test_main_happy_path(mock_tracker, mock_get_dbt_manifest, tmpdir):
    # Setup a fake file and manifest
    path = tmpdir.join("file.sql")
    path.write("select 1")
    manifest = {
        "nodes": {
            "test_model": {
                "original_file_path": str(path),
                "depends_on": {"nodes": []},  # No dependencies
            }
        }
    }
    mock_get_dbt_manifest.return_value = manifest

    # Simulate command line arguments
    argv = ["--is_test", str(path)]
    ret = main(argv)

    assert ret == 0
    mock_get_dbt_manifest.assert_called_once()
    mock_tracker.assert_called_once()


@patch("dbt_checkpoint.check_script_ref_and_source.get_dbt_manifest")
def test_main_json_open_error(mock_get_dbt_manifest, tmpdir):
    # Make the mocked function raise an error
    mock_get_dbt_manifest.side_effect = JsonOpenError()

    path = tmpdir.join("file.sql")
    path.write("select 1")

    argv = [str(path)]
    ret = main(argv)

    assert ret == 1
    mock_get_dbt_manifest.assert_called_once()