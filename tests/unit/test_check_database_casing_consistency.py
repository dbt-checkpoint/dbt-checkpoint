from unittest.mock import MagicMock, patch

import pytest

from dbt_checkpoint.check_database_casing_consistency import (
    _find_inconsistent_objects,
    check_database_casing_consistency,
    main,
)
from dbt_checkpoint.utils import JsonOpenError


@pytest.fixture
def manifest():
    return {
        "nodes": {
            "model.test_model": {
                "database": "TEST",
                "schema": "test",
                "name": "model_1",
                "alias": "model_1",
            }
        },
        "sources": {
            "source.balboa.credits_total": {
                "database": "test",
                "schema": "test",
                "name": "credits_total",
                "alias": "credits_total",
            },
        },
    }


@pytest.fixture
def catalog():
    return {
        "nodes": {
            "model.test_model": {
                "metadata": {
                    "database": "TEST",
                    "schema": "test",
                    "name": "model_1",
                    "alias": "model_1",
                }
            }
        },
        "sources": {
            "source.balboa.credits_total": {
                "metadata": {
                    "database": "test",
                    "schema": "test",
                    "name": "credits_total",
                    "alias": "credits_total",
                }
            },
        },
    }


def test_find_inconsistent_objects(manifest, catalog):
    results = set()
    _find_inconsistent_objects(
        manifest["nodes"], catalog["nodes"], ["model.test_model"], results
    )
    assert len(results) == 0

    catalog["nodes"]["model.test_model"]["metadata"]["database"] = "TeSt"
    _find_inconsistent_objects(
        manifest["nodes"], catalog["nodes"], ["model.test_model"], results
    )
    assert len(results) == 1


def test_find_inconsistent_schema(manifest, catalog):
    """Tests for casing inconsistencies in the schema."""
    results = set()
    # Change the catalog's schema to a different case
    catalog["nodes"]["model.test_model"]["metadata"]["schema"] = "TEST"

    _find_inconsistent_objects(
        manifest["nodes"], catalog["nodes"], ["model.test_model"], results
    )
    assert len(results) == 1

    # Check that the error message contains the key parts
    result_message = list(results)[0]
    assert "TEST.test" in result_message
    assert "dbt project (manifest)" in result_message
    assert "TEST.TEST" in result_message
    assert "database (catalog)" in result_message


def test_check_database_casing_consistency(manifest, catalog):
    result = check_database_casing_consistency(manifest, catalog)
    assert result == 0

    catalog["nodes"]["model.test_model"]["metadata"]["database"] = "TeSt"
    result = check_database_casing_consistency(manifest, catalog)
    assert result == 1


@patch("dbt_checkpoint.check_database_casing_consistency.get_dbt_manifest")
@patch("dbt_checkpoint.check_database_casing_consistency.get_dbt_catalog")
@patch(
    "dbt_checkpoint.check_database_casing_consistency.argparse.ArgumentParser.parse_args"
)
def test_main(
    mock_parse_args,
    mock_get_dbt_catalog,
    mock_get_dbt_manifest,
):
    mock_parse_args.return_value = MagicMock()
    mock_get_dbt_manifest.return_value = {
        "nodes": {
            "model.test_model": {
                "database": "TEST",
                "schema": "test",
                "name": "model_1",
                "alias": "model_1",
            }
        },
        "sources": {
            "source.balboa.credits_total": {
                "database": "test",
                "schema": "test",
                "name": "credits_total",
                "alias": "credits_total",
            },
        },
    }
    mock_get_dbt_catalog.return_value = {
        "nodes": {
            "model.test_model": {
                "metadata": {
                    "database": "TEST",
                    "schema": "test",
                    "name": "model_1",
                    "alias": "model_1",
                }
            }
        },
        "sources": {
            "source.balboa.credits_total": {
                "metadata": {
                    "database": "test",
                    "schema": "test",
                    "name": "credits_total",
                    "alias": "credits_total",
                }
            },
        },
    }
    result = main()
    assert result == 0


@patch("dbt_checkpoint.check_database_casing_consistency.get_dbt_manifest")
@patch(
    "dbt_checkpoint.check_database_casing_consistency.argparse.ArgumentParser.parse_args"
)
def test_main_manifest_error(mock_parse_args, mock_get_dbt_manifest):
    """Tests the main function when manifest loading fails."""
    mock_parse_args.return_value = MagicMock()
    # Simulate an error when getting the manifest
    mock_get_dbt_manifest.side_effect = JsonOpenError()
    
    result = main()
    assert result == 1


@patch("dbt_checkpoint.check_database_casing_consistency.get_dbt_manifest")
@patch("dbt_checkpoint.check_database_casing_consistency.get_dbt_catalog")
@patch(
    "dbt_checkpoint.check_database_casing_consistency.argparse.ArgumentParser.parse_args"
)
def test_main_catalog_error(
    mock_parse_args, mock_get_dbt_catalog, mock_get_dbt_manifest
):
    """Tests the main function when catalog loading fails."""
    mock_parse_args.return_value = MagicMock()
    mock_get_dbt_manifest.return_value = {}  # Manifest loads successfully
    # Simulate an error when getting the catalog
    mock_get_dbt_catalog.side_effect = JsonOpenError()

    result = main()
    assert result == 1