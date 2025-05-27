import json
import pytest
from dbt_checkpoint.check_semantic_entities_have_required_fields import main
from dbt_checkpoint.utils import JsonOpenError


@pytest.mark.parametrize(
    "semantic_manifest, required_fields, expected_exit_code",
    [
        # All required fields present -> success
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "entities": [{"name": "ent1", "type": "primary", "expr": "id"}],
                    }
                ]
            },
            ["name", "type", "expr"],
            0,
        ),
        # Missing type -> failure
        (
            {
                "semantic_models": [
                    {"name": "model1", "entities": [{"name": "ent1", "expr": "id"}]}
                ]
            },
            ["name", "type", "expr"],
            1,
        ),
        # Missing name -> failure
        (
            {
                "semantic_models": [
                    {"name": "model1", "entities": [{"type": "primary", "expr": "id"}]}
                ]
            },
            ["name", "type", "expr"],
            1,
        ),
        # Missing expr -> failure
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "entities": [{"name": "ent1", "type": "primary"}],
                    }
                ]
            },
            ["name", "type", "expr"],
            1,
        ),
        # Missing multiple fields -> failure
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "entities": [{"type": "primary"}],  # missing name and expr
                    }
                ]
            },
            ["name", "type", "expr"],
            1,
        ),
        # No entities -> success
        (
            {"semantic_models": [{"name": "model1", "entities": []}]},
            ["name", "type", "expr"],
            0,
        ),
    ],
)
def test_main_various_entity_field_scenarios(
    semantic_manifest,
    required_fields,
    expected_exit_code,
    tmp_path,
    manifest_path_str,
):
    # Write semantic manifest to a temp file
    sm_path = tmp_path / "semantic_manifest.json"
    sm_path.write_text(json.dumps(semantic_manifest))

    # Build CLI arguments
    args = [
        "--manifest",
        manifest_path_str,
        "--semantic-manifest",
        str(sm_path),
        "--required-fields",
        *required_fields,
    ]

    # Invoke main and assert exit code
    exit_code = main(argv=args)
    assert exit_code == expected_exit_code


def test_main_fails_on_bad_semantic_manifest(tmp_path, monkeypatch, manifest_path_str):
    # point at a dummy file
    sm = tmp_path / "bad.json"
    sm.write_text("{}")
    # force the loader to blow up
    monkeypatch.setattr(
        "dbt_checkpoint.check_semantic_entities_have_required_fields.get_dbt_semantic_manifest",
        lambda args: (_ for _ in ()).throw(JsonOpenError("bang")),
    )
    code = main(
        argv=[
            "--manifest",
            manifest_path_str,
            "--semantic-manifest",
            str(sm),
            "--required-fields",
            "",
        ]
    )
    assert code == 1


def test_main_fails_on_bad_dbt_manifest(tmp_path, monkeypatch, manifest_path_str):
    sm = tmp_path / "good.json"
    sm.write_text("{}")
    # now have semantic load fine, but manifest loader fail
    monkeypatch.setattr(
        "dbt_checkpoint.check_semantic_entities_have_required_fields.get_dbt_manifest",
        lambda args: (_ for _ in ()).throw(JsonOpenError("ouch")),
    )
    code = main(
        argv=[
            "--manifest",
            manifest_path_str,
            "--semantic-manifest",
            str(sm),
            "--required-fields",
            "",
        ]
    )
    assert code == 1
