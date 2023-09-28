import pytest

from dbt_checkpoint.dbt_models_command import prepare_cmd


@pytest.mark.parametrize(
    "files,global_flags,cmd_flags,models,expected",
    [
        (["/aa/bb/cc.txt"], None, None, None, ["dbt", "run", "-m", "cc"]),
        (
            ["/aa/bb/cc.txt"],
            ["++debug", "++no-write-json"],
            None,
            None,
            ["dbt", "--debug", "--no-write-json", "run", "-m", "cc"],
        ),
        (
            ["/aa/bb/cc.txt"],
            None,
            ["+t", "prod"],
            None,
            ["dbt", "run", "-m", "cc", "-t", "prod"],
        ),
        (
            ["/aa/bb/cc.txt"],
            "",
            ["+t", "prod"],
            None,
            ["dbt", "run", "-m", "cc", "-t", "prod"],
        ),
        (
            ["/aa/bb/cc.txt"],
            None,
            None,
            [],
            ["dbt", "run", "-m", "cc"],
        ),
        (
            ["/aa/bb/cc.txt"],
            None,
            None,
            ["state:modified"],
            ["dbt", "run", "-m", "state:modified"],
        ),
    ],
)
def test_dbt_run_cmd(files, global_flags, cmd_flags, models, expected):
    result = prepare_cmd("run", files, global_flags, cmd_flags, models=models)
    assert result == expected


@pytest.mark.parametrize(
    "files,global_flags,cmd_flags,models,expected",
    [
        (["/aa/bb/cc.txt"], None, None, None, ["dbt", "compile", "-m", "cc"]),
        (
                ["/aa/bb/cc.txt"],
                ["++debug", "++no-write-json"],
                None,
                None,
                ["dbt", "--debug", "--no-write-json", "compile", "-m", "cc"],
        ),
        (
                ["/aa/bb/cc.txt"],
                None,
                ["+t", "prod"],
                None,
                ["dbt", "compile", "-m", "cc", "-t", "prod"],
        ),
        (
                ["/aa/bb/cc.txt"],
                "",
                ["+t", "prod"],
                None,
                ["dbt", "compile", "-m", "cc", "-t", "prod"],
        ),
        (
                ["/aa/bb/cc.txt"],
                None,
                None,
                [],
                ["dbt", "compile", "-m", "cc"],
        ),
        (
                ["/aa/bb/cc.txt"],
                None,
                None,
                ["state:modified"],
                ["dbt", "compile", "-m", "state:modified"],
        ),
    ],
)
def test_dbt_compile_cmd(files, global_flags, cmd_flags, models, expected):
    result = prepare_cmd("compile", files, global_flags, cmd_flags, models=models)
    assert result == expected



@pytest.mark.parametrize(
    "files,global_flags,cmd_flags,models,expected",
    [
        (["/aa/bb/cc.txt"], None, None, None, ["dbt", "test", "-m", "cc"]),
        (
                ["/aa/bb/cc.txt"],
                ["++debug", "++no-write-json"],
                None,
                None,
                ["dbt", "--debug", "--no-write-json", "test", "-m", "cc"],
        ),
        (
                ["/aa/bb/cc.txt"],
                None,
                ["+t", "prod"],
                None,
                ["dbt", "test", "-m", "cc", "-t", "prod"],
        ),
        (
                ["/aa/bb/cc.txt"],
                "",
                ["+t", "prod"],
                None,
                ["dbt", "test", "-m", "cc", "-t", "prod"],
        ),
        (
                ["/aa/bb/cc.txt"],
                None,
                None,
                [],
                ["dbt", "test", "-m", "cc"],
        ),
        (
                ["/aa/bb/cc.txt"],
                None,
                None,
                ["state:modified"],
                ["dbt", "test", "-m", "state:modified"],
        ),
    ],
)
def test_dbt_test_cmd(files, global_flags, cmd_flags, models, expected):
    result = prepare_cmd("test", files, global_flags, cmd_flags, models=models)
    assert result == expected
