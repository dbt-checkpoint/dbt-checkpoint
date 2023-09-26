from typing import Optional, Sequence

from dbt_checkpoint.dbt_models_command import parse_and_run


def main(argv: Optional[Sequence[str]] = None) -> int:
    return parse_and_run("build", argv)


if __name__ == "__main__":
    exit(main())
