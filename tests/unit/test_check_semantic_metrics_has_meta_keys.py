import json
import pytest
from dbt_checkpoint.check_semantic_metrics_has_meta_keys import main
from dbt_checkpoint.utils import JsonOpenError


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


def test_main_fails_on_bad_semantic_manifest(tmp_path, monkeypatch, manifest_path_str):
    # point at a dummy file
    sm = tmp_path / "bad.json"
    sm.write_text("{}")
    # force the loader to blow up
    monkeypatch.setattr(
        "dbt_checkpoint.check_semantic_metrics_has_meta_keys.get_dbt_semantic_manifest",
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
        "dbt_checkpoint.check_semantic_metrics_has_meta_keys.get_dbt_manifest",
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
