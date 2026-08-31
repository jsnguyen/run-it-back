import json
import io
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from run_it_back import Emitter, Pipeline, __version__, get_git_metadata


def test_version():
    assert __version__ == "0.1.0"


def test_git_metadata_records_commit_and_dirty_state():
    with patch("run_it_back.subprocess.run") as run:
        run.side_effect = [MagicMock(stdout="abc123\n"), MagicMock(stdout=" M file.py\n")]
        assert get_git_metadata("/project") == ("abc123", True)


def test_runtime_json_includes_git_metadata(tmp_path):
    pipeline = Pipeline.__new__(Pipeline)
    pipeline.run_id = "test_run"
    pipeline.config_dir = Path("/project")
    pipeline.pipeline_output_path = tmp_path
    pipeline.git_commit = "abc123"
    pipeline.git_dirty = True

    pipeline.write_runtime_json()

    runtime = json.loads((tmp_path / "runtime.json").read_text())
    assert runtime["git_commit"] == "abc123"
    assert runtime["git_dirty"] is True


def test_log_file_is_created_in_pipeline_output_directory(tmp_path):
    pipeline = Pipeline.__new__(Pipeline)
    pipeline.filepath = Path("/project/configs/science_pipeline.toml")
    pipeline.pipeline_output_path = tmp_path / "pipeline_output" / "science_pipeline"

    with patch("run_it_back.logging.basicConfig") as basic_config:
        pipeline.init_logger()

    assert basic_config.call_args.kwargs["filename"] == pipeline.pipeline_output_path / "science_pipeline.log"


def test_stage_stdout_is_tee_to_log_without_stderr():
    pipeline = Pipeline.__new__(Pipeline)
    log = MagicMock()
    pipeline.emit = Emitter(log=log, echo=False)
    stage = MagicMock()

    def run_stage():
        print("useful output")
        print("tqdm-like progress", file=sys.stderr)
        return 42

    stage.run_stage.side_effect = run_stage
    terminal_stdout = io.StringIO()
    terminal_stderr = io.StringIO()

    with redirect_stdout(terminal_stdout), redirect_stderr(terminal_stderr):
        result = pipeline._execute_stage(stage)

    assert result == 42
    assert terminal_stdout.getvalue() == "useful output\n"
    assert terminal_stderr.getvalue() == "tqdm-like progress\n"
    log.info.assert_called_once_with("useful output")


def test_pipeline_logs_exception_before_reraising():
    pipeline = Pipeline.__new__(Pipeline)
    log = MagicMock()
    pipeline.emit = Emitter(log=log, echo=False)
    pipeline._run = MagicMock(side_effect=ValueError("boom"))

    with pytest.raises(ValueError, match="boom"):
        pipeline.run()

    log.exception.assert_called_once_with("-> Pipeline failed")
