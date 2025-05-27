import json
import pytest
from dbt_checkpoint.check_semantic_dimensions_has_meta_keys import main


@pytest.mark.parametrize(
    "semantic_manifest, meta_keys, allow_extra, expected_exit_code",
    [
        # Only allowed key present -> success
        (
            {
                "semantic_models": [
                    {
                        "name": "account",
                        "dimensions": [
                            {"name": "platform", "config": {"meta": {"foo": "val"}}}
                        ],
                    }
                ]
            },
            "foo",
            False,
            0,
        ),
        # Missing required key -> failure
        (
            {
                "semantic_models": [
                    {
                        "name": "account",
                        "dimensions": [{"name": "platform", "config": {"meta": {}}}],
                    }
                ]
            },
            "foo",
            False,
            1,
        ),
        # Extra key not allowed -> failure
        (
            {
                "semantic_models": [
                    {
                        "name": "account",
                        "dimensions": [
                            {
                                "name": "platform",
                                "config": {"meta": {"foo": "val", "bar": "val"}},
                            }
                        ],
                    }
                ]
            },
            "foo",
            False,
            1,
        ),
        # Extra key allowed -> success
        (
            {
                "semantic_models": [
                    {
                        "name": "account",
                        "dimensions": [
                            {
                                "name": "platform",
                                "config": {"meta": {"foo": "val", "bar": "val"}},
                            }
                        ],
                    }
                ]
            },
            "foo",
            True,
            0,
        ),
        # Extra key allowed but no required key -> success
        (
            {
                "semantic_models": [
                    {
                        "name": "account",
                        "dimensions": [
                            {
                                "name": "platform",
                                "config": {"meta": {"foo": "val", "bar": "val"}},
                            }
                        ],
                    }
                ]
            },
            "foo",
            True,
            0,
        ),
        # Model without dimensions -> success
        (
            {"semantic_models": [{"name": "account", "dimensions": []}]},
            "foo",
            False,
            0,
        ),
    ],
)
def test_main_various_meta_key_scenarios(
    semantic_manifest,
    meta_keys,
    allow_extra,
    expected_exit_code,
    tmp_path,
    manifest_path_str,
):
    # Write out the semantic manifest to a temp file
    sm_path = tmp_path / "semantic_manifest.json"
    sm_path.write_text(json.dumps(semantic_manifest))

    # Build CLI arguments
    args = [
        "--manifest",
        manifest_path_str,
        "--semantic-manifest",
        str(sm_path),
        "--meta-keys",
        meta_keys,
    ]
    if allow_extra:
        args.append("--allow-extra-keys")

    # Invoke main and assert exit code
    result = main(argv=args)
    assert result == expected_exit_code
