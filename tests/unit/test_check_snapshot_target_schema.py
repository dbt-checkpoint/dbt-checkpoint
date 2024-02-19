import pytest

from dbt_checkpoint.check_snapshot_has_target_schema import main


def test_check_snapshot_target_schema():

    result = main(
        argv=[
            "in_snapshot_target_schema.sql",
            "--manifest",
            "test-manifest.json",
        ],
    )
    assert result == 0


def test_check_snapshot_target_schema_fail():

    result = main(
        argv=[
            "in_snapshot_target_schema_fail.sql",
            "--manifest",
            "test-manifest.json",
        ],
    )
    assert result == 1
