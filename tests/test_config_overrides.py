import json

import pytest

from run_it_back import Pipeline, RunItBackError, apply_config_overrides


def test_apply_config_overrides_uses_toml_values():
    config = {
        "pipeline": {"use_unique_path": True},
        "context": {"program": "OLD", "bounds": [-5, -2]},
    }

    applied = apply_config_overrides(config, [
        "pipeline.use_unique_path=false",
        "context.program='GTO1200'",
        "context.bounds=[-6, -1]",
    ])

    assert config["pipeline"]["use_unique_path"] is False
    assert config["context"]["program"] == "GTO1200"
    assert config["context"]["bounds"] == [-6, -1]
    assert applied["pipeline.use_unique_path"] is False


def test_apply_config_overrides_rejects_unknown_keys_and_invalid_values():
    config = {"pipeline": {"use_unique_path": True}}

    with pytest.raises(RunItBackError, match="Unknown config override key"):
        apply_config_overrides(config, ["pipeline.missing=false"])
    with pytest.raises(RunItBackError, match="Invalid TOML value"):
        apply_config_overrides(config, ["pipeline.use_unique_path=not a TOML value"])


def test_pipeline_records_and_restores_config_overrides(tmp_path):
    stages_path = tmp_path / "stages"
    stages_path.mkdir()
    (stages_path / "make_context.py").write_text("def make_context(context: dict) -> dict:\n    return {}\n")
    (stages_path / "fit.py").write_text("def fit(context: dict = {}) -> None:\n    pass\n")

    pipeline_path = tmp_path / "pipeline.toml"
    pipeline_path.write_text(
        "[pipeline]\n"
        "name = 'Override Pipeline'\n"
        "pipeline_output_path = 'runs'\n"
        "stages_path = 'stages'\n"
        "use_unique_path = true\n\n"
        "[context]\n"
        "filepath = 'stages/make_context.py'\n\n"
        "[stages.fit]\n"
        "filepath = 'stages/fit.py'\n"
        "inputs = []\n"
        "outputs = []\n"
    )

    overrides = ["pipeline.use_unique_path=false"]
    pipeline = Pipeline(pipeline_path, overrides=overrides)
    runtime = json.loads((pipeline.pipeline_output_path / "runtime.json").read_text())
    loaded = Pipeline.load_run(pipeline.pipeline_output_path)

    assert pipeline.pipeline_output_path == tmp_path / "runs" / "override_pipeline"
    assert pipeline.pipeline_parameters["use_unique_path"] is False
    assert runtime["config_overrides"] == overrides
    assert loaded.pipeline_parameters["use_unique_path"] is False
    assert loaded.config_overrides == overrides
