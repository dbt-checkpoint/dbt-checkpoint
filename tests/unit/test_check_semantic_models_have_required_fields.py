import json
import pytest
from dbt_checkpoint.check_semantic_models_have_required_fields import main


@pytest.mark.parametrize(
    "semantic_manifest, required_fields, expected_exit_code",
    [
        # All required fields present, no measures -> success
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "description": "desc",
                        "node_relation": "rel",
                        "defaults": {},
                        "measures": [],
                    }
                ]
            },
            ["description", "model"],
            0,
        ),
        # Missing description -> failure
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "node_relation": "rel",
                        "defaults": {},
                        "measures": [],
                    }
                ]
            },
            ["description", "model"],
            1,
        ),
        # Missing node_relation -> failure
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "description": "desc",
                        "defaults": {},
                        "measures": [],
                    }
                ]
            },
            ["description", "model"],
            1,
        ),
        # Missing both description and node_relation -> failure
        (
            {"semantic_models": [{"name": "model1", "defaults": {}, "measures": []}]},
            ["description", "model"],
            1,
        ),
        # With measures but missing agg_time_dimension -> failure
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "description": "desc",
                        "node_relation": "rel",
                        "defaults": {},
                        "measures": [{"name": "m1"}],
                    }
                ]
            },
            ["description", "model"],
            1,
        ),
        # With measures and agg_time_dimension present -> success
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "description": "desc",
                        "node_relation": "rel",
                        "defaults": {"agg_time_dimension": "date_day"},
                        "measures": [{"name": "m1"}],
                    }
                ]
            },
            ["description", "model"],
            0,
        ),
        # Custom required field missing -> failure
        (
            {
                "semantic_models": [
                    {
                        "name": "model1",
                        "description": "desc",
                        "node_relation": "rel",
                        "defaults": {},
                        "measures": [],
                    }
                ]
            },
            ["description", "model", "nonexistent"],
            1,
        ),
    ],
)
def test_main_various_model_field_scenarios(
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
