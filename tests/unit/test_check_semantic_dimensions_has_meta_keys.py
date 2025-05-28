import json
import pytest
from dbt_checkpoint.check_semantic_dimensions_has_meta_keys import main
from dbt_checkpoint.utils import JsonOpenError


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


def test_main_fails_on_bad_semantic_manifest(tmp_path, monkeypatch, manifest_path_str):
    # point at a dummy file
    sm = tmp_path / "bad.json"
    sm.write_text("{}")
    # force the loader to blow up
    monkeypatch.setattr(
        "dbt_checkpoint.check_semantic_dimensions_has_meta_keys.get_dbt_semantic_manifest",
        lambda args: (_ for _ in ()).throw(JsonOpenError("bang")),
    )
    code = main(
        argv=[
            "--manifest",
            manifest_path_str,
            "--semantic-manifest",
            str(sm),
            "--meta-keys",
            "",
        ]
    )
    assert code == 1


def test_main_fails_on_bad_dbt_manifest(tmp_path, monkeypatch, manifest_path_str):
    sm = tmp_path / "good.json"
    sm.write_text("{}")
    # now have semantic load fine, but manifest loader fail
    monkeypatch.setattr(
        "dbt_checkpoint.check_semantic_dimensions_has_meta_keys.get_dbt_manifest",
        lambda args: (_ for _ in ()).throw(JsonOpenError("ouch")),
    )
    code = main(
        argv=[
            "--manifest",
            manifest_path_str,
            "--semantic-manifest",
            str(sm),
            "--meta-keys",
            "",
        ]
    )
    assert code == 1
