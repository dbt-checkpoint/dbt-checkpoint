from unittest.mock import MagicMock, patch

import pytest

from dbt_checkpoint.check_database_casing_consistency import (
    _find_inconsistent_objects,
    check_database_casing_consistency,
    main,
)


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
    class DummyArgs:
        def __init__(self):
            self.config = "dummy_config_path"
            self.other_arg = "some_value"
            self.is_test = True

    mock_parse_args.return_value = DummyArgs()
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
