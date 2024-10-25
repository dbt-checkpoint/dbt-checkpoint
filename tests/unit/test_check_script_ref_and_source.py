from unittest.mock import patch

import pytest

from dbt_checkpoint.check_script_ref_and_source import check_refs_sources, main
from dbt_checkpoint.utils import get_json

# Input, expected return value, expected output


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
    manifest["nodes"]["test_model"]["original_file_path"] = str(path)
    hook_properties = check_refs_sources(paths=[path], manifest=manifest)

    ret = hook_properties.get("status_code")

    assert ret == expected_status_code
