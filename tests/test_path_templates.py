import json
import sys
from shutil import copy2, copytree

import pytest

from run_it_back import Pipeline, RunItBackError, Stage, check_files_exist


def test_stage_expands_path_and_string_context_values(tmp_path):
    data_path = tmp_path / "data"
    data_path.mkdir()
    (data_path / "input.fits").touch()

    config = {
        "context": {"DATA_PATH": data_path, "program": "GTO1200"},
        "inputs": ["${DATA_PATH}/*.fits", "${DATA_PATH}/${program}.pkl"],
        "outputs": ["results/${program}.pkl"],
    }
    stage = Stage("fit", config, tmp_path / "run")

    assert stage.inputs_config_raw == config["inputs"]
    assert stage.inputs_files == [f"{data_path}/*.fits", f"{data_path}/GTO1200.pkl"]
    assert stage.outputs_files == ["results/GTO1200.pkl"]
    assert check_files_exist(stage.inputs_files[:1], stage.pipeline_output_path) == []


def test_stage_template_missing_key_has_clear_error(tmp_path):
    pattern = "${MISSING_PATH}/input.pkl"

    with pytest.raises(RunItBackError) as error:
        Stage("fit", {"context": {}, "inputs": [pattern]}, tmp_path)

    message = str(error.value)
    assert "fit" in message
    assert pattern in message
    assert "MISSING_PATH" in message


def test_bare_relative_paths_remain_run_relative(tmp_path):
    run_path = tmp_path / "run"
    results_path = run_path / "results"
    results_path.mkdir(parents=True)
    (results_path / "output.pkl").touch()

    assert check_files_exist(["results/output.pkl"], run_path) == []
    assert check_files_exist(["results/*.pkl"], run_path) == []


def test_file_checks_expand_tilde_and_environment_variables(tmp_path, monkeypatch):
    home_path = tmp_path / "home"
    env_path = tmp_path / "external"
    home_path.mkdir()
    env_path.mkdir()
    (home_path / "home.pkl").touch()
    (env_path / "external.pkl").touch()

    monkeypatch.setenv("HOME", str(home_path))
    monkeypatch.setenv("RIB_TEST_PATH", str(env_path))

    assert check_files_exist(["~/home.pkl", "$RIB_TEST_PATH/*.pkl"], tmp_path / "run") == []


def test_pipeline_and_loaded_run_expand_templates_after_make_context(tmp_path):
    stages_path = tmp_path / "stages"
    data_path = tmp_path / "external"
    stages_path.mkdir()
    data_path.mkdir()
    (data_path / "input.fits").touch()

    (tmp_path / "path_helper.py").write_text(
        "from pathlib import Path\n\n"
        "def get_data_path(context):\n"
        "    return (context['config_dir'] / context['data_dir']).resolve()\n"
    )
    (stages_path / "make_context.py").write_text(
        "from path_helper import get_data_path\n\n"
        "def make_context(context: dict) -> dict:\n"
        "    return {'DATA_PATH': get_data_path(context)}\n"
    )
    (stages_path / "fit.py").write_text("def fit(context: dict = {}) -> None:\n    pass\n")
    pipeline_path = tmp_path / "pipeline.toml"
    pipeline_path.write_text(
        "[pipeline]\n"
        "name = 'Template Pipeline'\n"
        "pipeline_output_path = 'runs'\n"
        "stages_path = 'stages'\n"
        "use_unique_path = true\n\n"
        "[context]\n"
        "filepath = 'stages/make_context.py'\n"
        "data_dir = 'external'\n\n"
        "[stages.fit]\n"
        "filepath = 'stages/fit.py'\n"
        "inputs = ['${DATA_PATH}/*.fits']\n"
        "outputs = ['results/output.pkl']\n"
    )

    try:
        pipeline = Pipeline(pipeline_path)
        loaded = Pipeline.load_run(pipeline.pipeline_output_path)

        assert pipeline.stages[0].inputs_files == [f"{data_path}/*.fits"]
        assert loaded.stages[0].inputs_files == [f"{data_path}/*.fits"]

        relocated_root = tmp_path / "relocated"
        relocated_stages_path = relocated_root / "stages"
        relocated_data_path = relocated_root / "external"
        relocated_stages_path.mkdir(parents=True)
        relocated_data_path.mkdir()
        (relocated_data_path / "input.fits").touch()
        copy2(tmp_path / "path_helper.py", relocated_root / "path_helper.py")
        copy2(stages_path / "make_context.py", relocated_stages_path / "make_context.py")
        copy2(stages_path / "fit.py", relocated_stages_path / "fit.py")

        relocated_run_path = relocated_root / "runs" / pipeline.pipeline_output_path.name
        copytree(pipeline.pipeline_output_path, relocated_run_path)

        runtime_path = relocated_run_path / "runtime.json"
        runtime = json.loads(runtime_path.read_text())
        runtime["config_dir"] = str(tmp_path / "missing_remote_project")
        runtime_path.write_text(json.dumps(runtime, indent=2))
        sys.modules.pop("path_helper", None)

        relocated = Pipeline.load_run(relocated_run_path, config_dir=relocated_root)

        assert relocated.config_dir == relocated_root.resolve()
        assert relocated.pipeline_output_path == relocated_run_path.resolve()
        assert relocated.stages[0].filepath == relocated_stages_path / "fit.py"
        assert relocated.stages[0].inputs_files == [f"{relocated_data_path}/*.fits"]
    finally:
        sys.modules.pop("path_helper", None)
        excluded_paths = {str(tmp_path), str(stages_path), str(tmp_path / "relocated"), str(tmp_path / "relocated" / "stages")}
        sys.path[:] = [entry for entry in sys.path if entry not in excluded_paths]
