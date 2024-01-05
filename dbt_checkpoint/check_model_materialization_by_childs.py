"""Pre-commit hook that compares the number of childs with a given treshold to
improve on the chosen materialization of the models."""
import argparse
from typing import Any, Dict, List

from dbt_checkpoint.utils import (
    JsonOpenError,
    add_default_args,
    get_dbt_manifest,
    get_model_sqls,
    get_models,
    get_parent_childs,
)


def check_model_materialization_by_childs(
    paths: List[str],
    manifest: Dict[str, Any],
    threshold_childs: int,
    include_disabled: bool = False,
) -> int:
    """Compares the childs of a model with its materialization.

    :param paths: Paths to the sql files of the models.
    :param manifest: Manifest as a dictionary containing the dbt project metadata.
    :param threshold_childs: Threshold from which onwards the materialization should
    be changed.
    :return: Status code of the check.
    """
    status_code = 0
    sqls = get_model_sqls(paths, manifest, include_disabled)
    filenames = set(sqls.keys())

    # Prepare the manifest to extract the materialization per model.
    nodes = manifest.get("nodes", {})
    models = get_models(manifest, filenames, include_disabled=include_disabled)

    for model in models:
        nr_childs = len(
            list(
                get_parent_childs(
                    manifest=manifest,
                    obj=model,
                    manifest_node="child_map",
                    node_types=["model"],
                )
            )
        )
        model_materialization = (
            nodes.get(model.model_id).get("config").get("materialized")
        )

        # Compare the number of childs with the threshold.
        if nr_childs > threshold_childs and model_materialization == "view":
            status_code = 1
            print(
                f"{model.model_name}: "
                f"has {nr_childs} childs, but the materialization is "
                f"{model_materialization}. Consider changing the materialization to "
                "table or incremental.",
            )
        elif nr_childs <= threshold_childs and model_materialization != "view":
            status_code = 1
            print(
                f"{model.model_name}: "
                f"has {nr_childs} childs, but the materialization is "
                f"{model_materialization}. Consider changing the materialization to "
                "view or ephemeral.",
            )

    return status_code


def main(argv: List[str] = None) -> int:
    """Run the pre-commit hook to check the materialization of the models.

    :param args: Command line arguments that are parsed, defaults to None.
    :return: The command line exit code.
    """
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--threshold-childs",
        type=int,
        default=5,
        help="Number of child threshold to change the materialization.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as error_message:
        print(f"Unable to load manifest file ({error_message})")
        return 1

    status_code = check_model_materialization_by_childs(
        paths=args.filenames,
        manifest=manifest,
        threshold_childs=args.threshold_childs,
        include_disabled=args.include_disabled,
    )

    return status_code


if __name__ == "__main__":
    exit(main())
