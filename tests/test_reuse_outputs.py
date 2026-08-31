import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from run_it_back import Pipeline, Stage


def make_pipeline(tmp_path, stages):
    pipeline = Pipeline.__new__(Pipeline)
    pipeline.run_id = "new_run"
    pipeline.config_dir = tmp_path
    pipeline.pipeline_output_path = tmp_path / "new_run"
    pipeline.pipeline_output_path.mkdir()
    pipeline.git_commit = "new_commit"
    pipeline.git_dirty = False
    pipeline.stages = [Stage(name, {"outputs": outputs}, pipeline.pipeline_output_path) for name, outputs in stages]
    pipeline.emit = MagicMock()
    return pipeline


def make_source_run(tmp_path):
    source_run = tmp_path / "source_run"
    results_path = source_run / "results"
    results_path.mkdir(parents=True)
    (source_run / "runtime.json").write_text(json.dumps({"run_id": "source_id", "git_commit": "source_commit", "git_dirty": True, "config_overrides": ["context.value=1"]}))
    (results_path / "stage_one.pkl").write_text("one")
    (results_path / "stage_two_a.pkl").write_text("two a")
    (results_path / "stage_two_b.pkl").write_text("two b")
    (results_path / "stage_three.pkl").write_text("three")
    return source_run


def test_reuse_outputs_links_prior_stage_outputs_and_records_metadata(tmp_path):
    source_run = make_source_run(tmp_path)
    pipeline = make_pipeline(tmp_path, [
        ("one", ["results/stage_one.pkl"]),
        ("two", ["results/stage_two_*.pkl"]),
        ("three", ["results/stage_three.pkl"]),
    ])

    pipeline.reuse_outputs(source_run, through_stage_index=2)

    for filename in ("stage_one.pkl", "stage_two_a.pkl", "stage_two_b.pkl"):
        source = source_run / "results" / filename
        reused = pipeline.pipeline_output_path / "results" / filename
        assert reused.read_text() == source.read_text()
        assert reused.samefile(source)
    assert not (pipeline.pipeline_output_path / "results" / "stage_three.pkl").exists()

    runtime = json.loads((pipeline.pipeline_output_path / "runtime.json").read_text())
    assert runtime["source_run"] == "source_run"
    assert runtime["source_run_id"] == "source_id"
    assert runtime["source_git_commit"] == "source_commit"
    assert runtime["source_git_dirty"] is True
    assert runtime["source_config_overrides"] == ["context.value=1"]
    assert runtime["reused_through_stage"] == 2
    assert runtime["reused_file_count"] == 3


def test_reuse_outputs_warns_and_continues_when_source_output_is_missing(tmp_path):
    source_run = make_source_run(tmp_path)
    pipeline = make_pipeline(tmp_path, [
        ("one", ["results/stage_one.pkl"]),
        ("missing", ["results/missing.pkl"]),
    ])

    pipeline.reuse_outputs(source_run, through_stage_index=2)

    assert (pipeline.pipeline_output_path / "results" / "stage_one.pkl").exists()
    pipeline.emit.warning.assert_called_once_with("-> Source run is missing output results/missing.pkl from stage missing; skipping")


def test_reuse_outputs_copies_when_hard_links_are_unavailable(tmp_path):
    source_run = make_source_run(tmp_path)
    pipeline = make_pipeline(tmp_path, [("one", ["results/stage_one.pkl"])])

    with patch("run_it_back.os.link", side_effect=OSError):
        pipeline.reuse_outputs(source_run, through_stage_index=1)

    source = source_run / "results" / "stage_one.pkl"
    reused = pipeline.pipeline_output_path / "results" / "stage_one.pkl"
    assert reused.read_text() == source.read_text()
    assert not reused.samefile(source)
