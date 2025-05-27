import json
import pytest
from dbt_checkpoint.check_semantic_metrics_has_meta_keys import main


@pytest.mark.parametrize(
    "semantic_manifest, meta_keys, allow_extra_keys, expected_exit_code",
    [
        # Only required key present -> success
        (
            {"metrics": [{"name": "m1", "config": {"meta": {"foo": "val"}}}]},
            ["foo"],
            False,
            0,
        ),
        # Missing required key -> failure
        (
            {"metrics": [{"name": "m1", "config": {"meta": {}}}]},
            ["foo"],
            False,
            1,
        ),
        # Extra key not allowed -> failure
        (
            {
                "metrics": [
                    {"name": "m1", "config": {"meta": {"foo": "val", "bar": "val"}}}
                ]
            },
            ["foo"],
            False,
            1,
        ),
        # Extra key allowed -> success
        (
            {
                "metrics": [
                    {"name": "m1", "config": {"meta": {"foo": "val", "bar": "val"}}}
                ]
            },
            ["foo"],
            True,
            0,
        ),
        # Multiple required keys present -> success
        (
            {"metrics": [{"name": "m1", "config": {"meta": {"foo": "v", "bar": "v"}}}]},
            ["foo", "bar"],
            False,
            0,
        ),
        # Missing one of multiple required keys -> failure
        (
            {"metrics": [{"name": "m1", "config": {"meta": {"foo": "v"}}}]},
            ["foo", "bar"],
            False,
            1,
        ),
    ],
)
def test_main_various_meta_key_scenarios(
    semantic_manifest,
    meta_keys,
    allow_extra_keys,
    expected_exit_code,
    tmp_path,
    manifest_path_str,
):
    # Write out the semantic manifest
    sm_path = tmp_path / "semantic_manifest.json"
    sm_path.write_text(json.dumps(semantic_manifest))

    # Build CLI args
    args = [
        "--manifest",
        manifest_path_str,
        "--semantic-manifest",
        str(sm_path),
        "--meta-keys",
        *meta_keys,
    ]
    if allow_extra_keys:
        args.append("--allow-extra-keys")

    # Invoke and assert
    exit_code = main(argv=args)
    assert exit_code == expected_exit_code
