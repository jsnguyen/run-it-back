# run-it-back

An end-to-end orchestrator for running data analysis pipelines in a repeatable way.

Each run creates a unique output directory, copies the pipeline TOML into that
directory, writes a small `runtime.json`, and logs pipeline execution. Pipeline
stages are regular Python functions loaded from file paths in the TOML.

Currently a work in progress.

Validation right now only works for file existence, but the plan is to add more robust type checking and better error messages.

## Command Line Interface

Run the whole pipeline:

```bash
rib pipeline.toml
```

Run only one stage of the pipeline:

```bash
rib pipeline.toml --stages 1
```

Run from a stage to the end:

```bash
rib pipeline.toml --stages 5:
```

Run through a stage:

```bash
rib pipeline.toml --stages :7
```

Run a stage range:

```bash
rib pipeline.toml --stages 2:5
```

Stage numbers are 1-indexed and inclusive in the CLI. Internally they are
converted to Python slice bounds.

Useful flags:

```bash
rib pipeline.toml --skip-validation
```

Skips some of the validation aspects of the pipeline, not totally working right now.

```bash
rib pipeline.toml --time-stages
rib pipeline-toml -t
```

Times each of the stages and prints the times.

```bash
rib pipeline-toml --run-aux N
```

Runs the auxillary stage for stage N only.

## Logging

Each pipeline writes a log named after its TOML file in the pipeline output directory. RIB messages and stage stdout are written to both the terminal and the log. Stage stderr remains terminal-only so progress displays such as `tqdm` do not fill the log. If pipeline initialization or execution raises an exception, RIB writes the full traceback as the final log entry before re-raising it.

## Definitions

- `context`: A dictionary passed to every stage function that accepts a
  `context` keyword argument.
- `make_context`: A special initialization stage configured in `[context]`.
  It runs during `Pipeline` initialization and must return a dictionary. That
  dictionary is merged into `context`.
- `pipeline_output_path`: The unique run output directory. This is added to
  `context` before `make_context` runs.
- `config_dir`: The directory containing the original pipeline TOML. This is
  added to `context` so context-building code can resolve input files without
  depending on the current working directory.
- `aux_stages`: Optional stages attached to a primary stage. They run after
  the primary stage and should be used for side-effect-only work such as plots
  or diagnostics.

## Schema

```toml
[pipeline]
name = "Data Analysis Pipeline"
pipeline_output_path = "pipeline_output"
stages_path = "stages"

[context]
filepath = "stages/make_context.py"
target = "HR 8799"
epoch = 2024.1
DATA_PATH = "../data"

[stages.load_data]
filepath = "stages/load_data.py"
params = { name = "test" }
inputs = []
outputs = ["data", "const"]

[stages.calibrate_data]
filepath = "stages/calibrate_data.py"
func_name = "alt_calibrate"
inputs = ["data", "const"]
outputs = ["data_calibrated", "number"]
aux_stages = ["plot_calibration"]

[stages.analyze_data]
filepath = "stages/analyze_data.py"
inputs = ["data_calibrated", "number"]
outputs = ["figs/calibrated_data_heatmap.png"]

[aux_stages.plot_calibration]
filepath = "stages/plot_calibration.py"
func_name = "plot_calibration"
inputs = ["results/calibration.pkl"]
```

If `func_name` is omitted, the function name is assumed to match the stage name.
File inputs and outputs are currently detected by the presence of a `.` in the
configured string.

## Stage Functions

Stages can accept regular positional/keyword inputs and can optionally accept
`context`:

```python
def load_data(name: str, context: dict) -> tuple:
    ...
```

The `make_context` function should return a dictionary:

```python
from pathlib import Path

def make_context(context: dict) -> dict:
    data_path = Path(context["DATA_PATH"])
    if not data_path.is_absolute():
        data_path = context["config_dir"] / data_path

    results_path = context["pipeline_output_path"] / "results"
    results_path.mkdir(exist_ok=True, parents=True)

    return {
        "DATA_PATH": data_path.resolve(),
        "RESULTS_PATH": results_path,
    }
```

Prefer resolving input paths relative to `context["config_dir"]` and output
paths relative to `context["pipeline_output_path"]`.

## Reloading Runs

Each run directory contains:

- the copied pipeline TOML
- `runtime.json`
- the log file
- pipeline outputs

For post-processing an existing run:

```python
from run_it_back import Pipeline

pipeline = Pipeline.load_run("pipeline_output/data_analysis_pipeline_20260508_120000_abcd")
```

`load_run` reconstructs the pipeline from the run directory without creating a
new run directory and without logging by default. It re-runs `make_context`, so
that function should be idempotent.

Pass `echo=True` if you want terminal output while loading:

```python
pipeline = Pipeline.load_run("pipeline_output/data_analysis_pipeline_20260508_120000_abcd", echo=True)
```

`runtime.json` is intentionally small. It stores metadata needed to reload a
run, such as `run_id` and `config_dir`; it is not intended to serialize the full
runtime `context`.

## TODO

- hashing of output files to check if they have been modified or changed
- better system for type checking? maybe use inspect or don't rely so much on the ast
