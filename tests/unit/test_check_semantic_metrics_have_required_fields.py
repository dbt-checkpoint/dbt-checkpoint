import json
import pytest
from dbt_checkpoint.check_semantic_metrics_have_required_fields import main
from dbt_checkpoint.utils import JsonOpenError


@pytest.mark.parametrize(
    "semantic_manifest, required_fields, expected_exit_code",
    [
        # All required fields present -> success
        (
            {
                "metrics": [
                    {
                        "name": "m1",
                        "type": "simple",
                        "description": "desc",
                        "label": "lbl",
                    }
                ]
            },
            ["name", "type", "description", "label"],
            0,
        ),
        # Missing name -> failure
        (
            {"metrics": [{"type": "simple", "description": "desc", "label": "lbl"}]},
            ["name", "type", "description", "label"],
            1,
        ),
        # Missing type -> failure
        (
            {"metrics": [{"name": "m1", "description": "desc", "label": "lbl"}]},
            ["name", "type", "description", "label"],
            1,
        ),
        # Missing description -> failure
        (
            {"metrics": [{"name": "m1", "type": "simple", "label": "lbl"}]},
            ["name", "type", "description", "label"],
            1,
        ),
        # Missing label -> failure
        (
            {"metrics": [{"name": "m1", "type": "simple", "description": "desc"}]},
            ["name", "type", "description", "label"],
            1,
        ),
        # Missing multiple fields -> failure
        (
            {"metrics": [{"description": "desc"}]},  # missing name, type, label
            ["name", "type", "description", "label"],
            1,
        ),
        # No metrics -> success
        (
            {"metrics": []},
            ["name", "type", "description", "label"],
            0,
        ),
    ],
)
def test_main_various_metric_field_scenarios(
    semantic_manifest,
    required_fields,
    expected_exit_code,
    tmp_path,
    manifest_path_str,
):
    # Write the semantic manifest to a temp file
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
        "dbt_checkpoint.check_semantic_metrics_have_required_fields.get_dbt_semantic_manifest",
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
        "dbt_checkpoint.check_semantic_metrics_have_required_fields.get_dbt_manifest",
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
