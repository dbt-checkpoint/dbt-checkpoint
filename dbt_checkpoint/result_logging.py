import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Dict

from dbt_checkpoint.utils import get_config_file


@dataclass
class ResultSummary:
    """A class to represent the summary of a check run."""

    name: str
    description: str
    status_code: int
    total: int
    passed: int
    execution_time: float

    def to_dict(self) -> Dict[str, Any]:
        """Return a dictionary representation of the ResultSummary object."""
        return {
            "name": self.name,
            "description": self.description,
            "status_code": self.status_code,
            "total": self.total,
            "passed": self.passed,
            "execution_time": self.execution_time,
        }

    def to_json(self) -> str:
        """Return a JSON representation of the ResultSummary object."""
        return json.dumps(self.to_dict())


class ResultLogger:
    """A class to log the results of a check run."""

    def __init__(self, script_args: Dict[str, Any]):
        self.script_args = script_args
        config_path = script_args.get("config")
        if config_path is None or not isinstance(config_path, str):
            raise ValueError("config_path must be a non-empty string")

        self.config = get_config_file(config_path)
        self.dbt_project_dir = self.config.get("dbt-project-dir")
        self.enable_logging = self.config.get("enable-logging", False)
        self.log_path = Path(self.config.get("log-path", "logs"))

    def log_result(self, result_summary: ResultSummary) -> None:
        if not self.enable_logging:
            return
        self.log_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        log_file = f"{self.log_path}/{result_summary.name}_{timestamp}.json"
        with open(log_file, "w") as f:
            f.write(result_summary.to_json())
        print(f"Result logged to {log_file}")
