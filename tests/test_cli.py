import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from run_it_back.cli import main


def test_from_run_reuses_outputs_before_selected_stage(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["rib", "pipeline.toml", "--from-run", "source_run", "--stages", "5:"])
    pipeline = MagicMock()

    with patch("run_it_back.cli.Pipeline", return_value=pipeline) as pipeline_class:
        main()

    pipeline_class.assert_called_once_with(Path("pipeline.toml"), skip_validation=False, overrides=[])
    pipeline.reuse_outputs.assert_called_once_with("source_run", through_stage_index=4)
    pipeline.run.assert_called_once_with(
        start_stage_index=4,
        end_stage_index=None,
        time_stages=False,
        run_aux_stage_index=None,
        run_standalone_stage=None,
    )


def test_from_run_failure_is_logged(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["rib", "pipeline.toml", "--from-run", "missing_run", "--stages", "5:"])
    pipeline = MagicMock()
    pipeline.reuse_outputs.side_effect = RuntimeError("missing source")

    with patch("run_it_back.cli.Pipeline", return_value=pipeline), pytest.raises(RuntimeError, match="missing source"):
        main()

    pipeline.emit.exception.assert_called_once_with("-> Reusing outputs failed")
    pipeline.run.assert_not_called()


def test_override_is_passed_to_pipeline(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["rib", "pipeline.toml", "--override", "pipeline.use_unique_path=false", "--override", "context.program='GTO1200'"])
    pipeline = MagicMock()

    with patch("run_it_back.cli.Pipeline", return_value=pipeline) as pipeline_class:
        main()

    pipeline_class.assert_called_once_with(Path("pipeline.toml"), skip_validation=False, overrides=["pipeline.use_unique_path=false", "context.program='GTO1200'"])
