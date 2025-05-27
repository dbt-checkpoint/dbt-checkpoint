import argparse
import os
import time
from typing import Sequence, Optional, Tuple, Dict, Any

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    add_meta_keys_args,
    get_dbt_manifest,
    get_dbt_semantic_manifest,
    red,
    yellow,
    SemanticModel,
    SemanticLayerMetric,
    get_semantic_models,
    get_semantic_layer_metrics,
)


def validate_meta_keys(
    actual: Sequence[str], expected: Sequence[str], allow_extra_keys: bool
) -> bool:
    """
    Compare actual vs expected meta keys.
    If allow_extra_keys is True, actual may include keys beyond expected.
    Otherwise, actual must exactly match expected.
    """
    actual_set = set(actual)
    expected_set = set(expected)
    return (
        expected_set.issubset(actual_set)
        if allow_extra_keys
        else actual_set == expected_set
    )


def check_semantic_metrics_meta_keys(
    models: Sequence[SemanticModel],
    metrics: Sequence[SemanticLayerMetric],
    meta_keys: Sequence[str],
    allow_extra_keys: bool,
) -> int:
    """
    Validate that each metric's referenced measure includes the required meta keys.

    Args:
        models: Sequence of SemanticModel objects (with measures).
        metrics: Sequence of SemanticLayerMetric objects.
        meta_keys: Required meta key names.
        allow_extra_keys: Whether extra keys in the measure's meta are permitted.

    Returns:
        status_code: 0 if all validations pass, 1 otherwise.
    """
    status_code = 0

    # Build lookup: measure_name -> (model_name, measure_dict)
    measure_map: Dict[str, Tuple[str, Dict[str, Any]]] = {}
    for model in models:
        for measure in model.measures:
            m_name = measure.get("name")
            if m_name:
                measure_map[m_name] = (model.name or "<unnamed>", measure)

    # Validate each metric
    for metric in metrics:
        metric_name = metric.name or "<unnamed>"
        # Extract referenced measure name
        measure_info: Dict[str, Any] = metric.type_params.get("measure", {}) or {}
        measure_name = measure_info.get("name")
        if not measure_name:
            status_code = 1
            print(
                f"{red('<unknown>')}: metric '{metric_name}' does not specify a measure"
            )
            continue

        # Lookup measure
        entry = measure_map.get(measure_name)
        if not entry:
            status_code = 1
            print(
                f"{red('<unknown>')}: metric '{metric_name}' references unknown measure '{measure_name}'"
            )
            continue

        model_name, measure = entry
        # Extract meta dict from measure config
        meta = measure.get("config", {}).get("meta", {})
        actual_keys = list(meta.keys())

        # Compare actual vs expected
        if not validate_meta_keys(actual_keys, meta_keys, allow_extra_keys):
            status_code = 1
            print(
                f"{red(model_name)}: metric '{metric_name}' (measure '{measure_name}') failed meta key validation"
            )
            missing = [k for k in meta_keys if k not in actual_keys]
            if missing:
                print(f"  - {yellow('missing: ' + ', '.join(missing))}")
            if not allow_extra_keys:
                extra = [k for k in actual_keys if k not in meta_keys]
                if extra:
                    print(f"  - {yellow('extra: ' + ', '.join(extra))}")

    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    """
    CLI entrypoint: parse args, load manifests, extract models and metrics,
    and validate measure meta keys for metrics.
    """
    parser = argparse.ArgumentParser(
        description="Check semantic metrics have required meta keys on their referenced measures"
    )
    add_default_args(parser)
    add_meta_keys_args(parser)
    args = parser.parse_args(argv)

    # Load semantic manifest
    try:
        semantic_manifest = get_dbt_semantic_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load semantic manifest file: {e}")
        return 1

    # Load dbt manifest for tracking context
    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load dbt manifest file: {e}")
        return 1

    # Extract dataclass objects
    models = list(get_semantic_models(semantic_manifest))
    metrics = list(get_semantic_layer_metrics(semantic_manifest))

    # Perform validation
    start_time = time.time()
    status_code = check_semantic_metrics_meta_keys(
        models=models,
        metrics=metrics,
        meta_keys=args.meta_keys,
        allow_extra_keys=args.allow_extra_keys,
    )
    elapsed = time.time() - start_time

    # Track hook execution
    tracker = dbtCheckpointTracking(script_args=vars(args))
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check semantic metrics have required meta keys on their referenced measures",
            "status": status_code,
            "execution_time": elapsed,
            "is_pytest": vars(args).get("is_test"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
