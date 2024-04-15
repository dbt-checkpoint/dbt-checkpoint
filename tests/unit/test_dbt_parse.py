from unittest.mock import patch

import pytest

from dbt_checkpoint.dbt_parse import main
from dbt_checkpoint.dbt_parse import prepare_cmd


def test_dbt_parse():
    with patch("dbt_checkpoint.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 0
        result = main(argv=[])
        assert result == 0


def test_dbt_parse_error():
    with patch("dbt_checkpoint.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            "stderr",
        )
        mock_popen.return_value.returncode = 1
        result = main(argv=[])
        assert result == 1


@pytest.mark.parametrize(
    "global_flags,cmd_flags,expected",
    [
        (None, None, ["dbt", "parse"]),
        (
            ["++debug", "++no-write-json"],
            None,
            [
                "dbt",
                "--debug",
                "--no-write-json",
                "parse",
            ],
        ),
        (
            None,
            ["+t", "prod"],
            ["dbt", "parse", "-t", "prod"],
        ),
        (
            "",
            ["+t", "prod"],
            ["dbt", "parse", "-t", "prod"],
        ),
    ],
)
def test_dbt_parse_cmd(global_flags, cmd_flags, expected):
    result = prepare_cmd(global_flags, cmd_flags)
    assert result == expected
