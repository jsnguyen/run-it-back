# run-it-back

An end-to-end orchestrator for running a data analysis pipeline in a repeatable way. Each pipeline can be represented as a directed acyclic graph (DAG).

Various checks go into validating the pipeline:
- String-based typed checking
- Counting the number of inputs/outputs
- Checking that the output files are created

Due to limitations in the AST, there are going to be edge cases that don't work or are not supported.

All functions need to have type annotations for the inputs and outputs. The outputs can be either variables or files. The functions must also be pure functions, meaning they should not have side effects and should always produce the same output for the same input.

Currently a work in progress!

## Definitions

- context: A dictionary containing global state that is passed to all stages in the pipeline. This is initialized in the beginning and has a special setup in the .toml file.

## Schema

``` toml
[pipeline]
name = "Data Analysis Pipeline"

[context]
filepath = "stages/make_context.py" # special filepath that is used to initialize the context. File must return a dictionary.

target = "HR 8799"
epoch = 2024.1

[stages.load_data]
filepath = "stages/load_data.py" # filepath, function to be executed is assumed to be the same name as the stage if func_name is not specified
params = { name = "test"} # keyword arguments
inputs = [] # positional arguments
outputs = ["data", "const"] # outputs

[stages.calibrate_data]
filepath = "stages/calibrate_data.py"
func_name = "alt_calibrate" # use an alternative function
inputs = ["data", "const"]
outputs = ["data_calibrated", "number"]

[stages.analyze_data]
filepath = "stages/analyze_data.py"
func_name = "analyze_data"
inputs = ["data_calibrated", "number"]
outputs = ["figs/calibrated_data_heatmap.png"] # files are autodetected by the .

```
