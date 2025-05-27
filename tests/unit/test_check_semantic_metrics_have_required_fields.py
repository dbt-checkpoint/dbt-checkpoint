import json
import pytest
from dbt_checkpoint.check_semantic_metrics_have_required_fields import main


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
