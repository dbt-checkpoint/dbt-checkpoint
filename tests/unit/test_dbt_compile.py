from unittest.mock import patch

import pytest

from dbt_checkpoint.dbt_compile import main, prepare_cmd


def test_dbt_compile():
    with patch("dbt_checkpoint.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 0
        result = main(("test",))
        assert result == 0


def test_dbt_compile_error():
    with patch("dbt_checkpoint.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            "stderr",
        )
        mock_popen.return_value.returncode = 1
        result = main(("test",))
        assert result == 1


@pytest.mark.parametrize(
    "files,global_flags,cmd_flags,models,expected",
    [
        (["/aa/bb/cc.txt"], None, None, None, ["dbt", "compile", "--select", "cc"]),
        (
            ["/aa/bb/cc.txt"],
            ["++debug", "++no-write-json"],
            None,
            None,
            ["dbt", "--debug", "--no-write-json", "compile", "--select", "cc"],
        ),
        (
            ["/aa/bb/cc.txt"],
            None,
            ["+t", "prod"],
            None,
            ["dbt", "compile", "--select", "cc", "-t", "prod"],
        ),
        (
            ["/aa/bb/cc.txt"],
            "",
            ["+t", "prod"],
            None,
            ["dbt", "compile", "--select", "cc", "-t", "prod"],
        ),
        (
            ["/aa/bb/cc.txt"],
            None,
            None,
            [],
            ["dbt", "compile", "--select", "cc"],
        ),
        (
            ["/aa/bb/cc.txt"],
            None,
            None,
            ["state:modified"],
            ["dbt", "compile", "--select", "state:modified"],
        ),
    ],
)
def test_dbt_compile_cmd(files, global_flags, cmd_flags, models, expected):
    result = prepare_cmd(files, global_flags, cmd_flags, models=models)
    assert result == expected
