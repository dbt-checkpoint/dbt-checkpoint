from unittest.mock import patch

from dbt_checkpoint.dbt_run import main


def test_dbt_run():
    with patch("dbt_checkpoint.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 0
        result = main(("test",))
        assert result == 0


def test_dbt_run_error():
    with patch("dbt_checkpoint.utils.subprocess.Popen") as mock_popen:
        mock_popen.return_value.communicate.return_value = (
            b"stdout",
            b"stderr",
        )
        mock_popen.return_value.returncode = 1
        result = main(("test",))
        assert result == 1

