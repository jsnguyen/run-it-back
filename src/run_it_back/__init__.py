__version__ = "0.1.0"
import warnings

import ast
import os
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path
from string import Template
import importlib.util
import inspect
import tomllib
import glob
from datetime import datetime
import secrets
from shutil import copy2
import logging
import json
import subprocess

SAVE_FUNC_NAMES = ("save", "savefig", "to_csv", "to_parquet", "to_hdf",
                   "to_excel", "to_json", "to_pickle", "to_feather",
                   "writeto", "write_to", "write", "savetxt", "savez",
                   "savez_compressed", "imwrite", "dump")

class RunItBackError(Exception):
    pass

def apply_config_overrides(config, overrides):
    applied = {}
    for expression in overrides or []:
        key, separator, raw_value = expression.partition("=")
        key = key.strip()
        raw_value = raw_value.strip()
        if not separator or not key or not raw_value:
            raise RunItBackError(f"Invalid config override '{expression}'; expected KEY=VALUE")

        try:
            value = tomllib.loads(f"value = {raw_value}")["value"]
        except tomllib.TOMLDecodeError as error:
            raise RunItBackError(f"Invalid TOML value in config override '{expression}': {error}") from error

        path = key.split(".")
        target = config
        for part in path[:-1]:
            if part not in target or not isinstance(target[part], dict):
                raise RunItBackError(f"Unknown config override key '{key}'")
            target = target[part]
        if path[-1] not in target:
            raise RunItBackError(f"Unknown config override key '{key}'")

        target[path[-1]] = value
        applied[key] = value
    return applied

def get_git_metadata(repo_path):
    kwargs = {"check": True, "capture_output": True, "text": True}
    try:
        commit = subprocess.run(["git", "-C", str(repo_path), "rev-parse", "HEAD"], **kwargs).stdout.strip()
        status = subprocess.run(["git", "-C", str(repo_path), "status", "--porcelain"], **kwargs).stdout
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None, None
    return commit, bool(status.strip())

class Stage:
    def __init__(self, name, config, pipeline_output_path, aux_stage_index=None):

        self.name = name

        self.filepath = config.get("filepath", None)

        self.pipeline_output_path = pipeline_output_path

        if self.filepath is not None:
            self.filepath = Path(self.filepath)
        else:
            self.filepath = Path(name).with_suffix('.py')

        self.func_name = config.get("func_name", name)

        self.params = config.get("params", None)

        self.inputs = None
        self.outputs = None

        self.inputs_config_raw = config.get("inputs", [])
        self.outputs_config_raw = config.get("outputs", [])

        self.context = config.get("context", {})

        self.inputs_config = expand_file_templates(self.inputs_config_raw, self.context, self.name, "input")
        self.outputs_config = expand_file_templates(self.outputs_config_raw, self.context, self.name, "output")

        self.inputs_files = [el for el in self.inputs_config if is_file(el)]
        self.outputs_files = [el for el in self.outputs_config if is_file(el)]

        self.inputs_type_str = []
        self.outputs_type_str = []

        self.aux_stage_index = aux_stage_index # if this is not None, then it is an aux stage belonging to stage number corresponding to the index

    def run_stage(self):
        if self.func_name is None:
            raise ValueError(f"Stage {self.name} is missing 'func_name' in config")

        missing_files = check_files_exist(self.inputs_files, self.pipeline_output_path)
        if missing_files != []:
            raise RunItBackError(f"Missing input files! {missing_files} for stage {self.name}")

        spec = importlib.util.spec_from_file_location(self.name, self.filepath)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        func = getattr(module, self.func_name)

        if has_kwarg(func, "context"):
            if self.inputs is None and self.params is None:
                res = func(context=self.context)
            elif self.inputs is None and self.params is not None:
                res = func(**self.params, context=self.context)
            elif self.inputs is not None and self.params is None:
                res = func(*self.inputs, context=self.context)
            else:
                res = func(*self.inputs, **self.params, context=self.context)
        else:
            if self.inputs is None and self.params is None:
                res = func()
            elif self.inputs is None and self.params is not None:
                res = func(**self.params)
            elif self.inputs is not None and self.params is None:
                res = func(*self.inputs)
            else:
                res = func(*self.inputs, **self.params)

        missing_files = check_files_exist(self.outputs_files, self.pipeline_output_path)
        if missing_files != []:
            warnings.warn(f"Missing output files! {missing_files}")

        return res

class Pipeline:
    '''
    Required entries:

    - [pipeline]
        - name
        - pipeline_output_path
        - stage_path

    - [context]
        - filepath (for make_context stage)

    - relative paths output to the pipeline output folder, prefer relative paths
    - absolute paths should be used for things that are effectively constant in the pipeline

    '''

    def __init__(self, filepath, skip_validation=False, overrides=None):
        print_run_it_back()
        print("="*64)
        print()

        self.filepath = Path(filepath).resolve()
        with open(self.filepath, "rb") as f:
            self.config = tomllib.load(f)
        self.config_overrides = list(overrides or [])
        self.config_override_values = apply_config_overrides(self.config, self.config_overrides)

        # context is the global state that gets passed to all stages
        # use the object context stage to initialize python objects
        self.context             = self.config["context"] # initialize as config dict
        self.pipeline_parameters = self.config["pipeline"] # these are the pipeline specific parameters
        self.config_dir = self.filepath.parent.resolve()
        self.context["config_dir"] = self.config_dir
        self.git_commit, self.git_dirty = get_git_metadata(self.config_dir)
        self.source_run = None
        self.source_run_id = None
        self.source_git_commit = None
        self.source_git_dirty = None
        self.source_config_overrides = None
        self.reused_through_stage = None
        self.reused_file_count = None

        self.generate_run_id()

        self.configure_pipeline_output()
        copy2(self.filepath, self.pipeline_output_path / self.filepath.name) # copy the pipeline file to the output directory for better record keeping

        self.write_runtime_json() # all runtime info needed is initialized up to here

        self.emit = self.init_logger()
        try:
            self.emit(f"-> Initializing {self.pipeline_parameters.get("name")}")
            self.emit(f"-> Pipeline output path: {self.pipeline_output_path}")
            if self.git_commit is None:
                self.emit("-> Git commit: unavailable")
            else:
                git_status = "dirty" if self.git_dirty else "clean"
                self.emit(f"-> Git commit: {self.git_commit} ({git_status})")
            for key, value in self.config_override_values.items():
                self.emit(f"-> Config override: {key} = {value!r}")

            self.configure_stages_path()

            self.make_context_stage()

            self.parse_stages()

            if not skip_validation:
                self.validate()
        except BaseException:
            self.emit.exception("-> Pipeline initialization failed")
            raise

    def generate_run_id(self):
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + secrets.token_hex(2) # add run id for better tracking of different runs, can be used in context if desired

    def configure_pipeline_output(self):
        '''
        Turns the output paths into absolute paths, also makes it a unique path
        '''

        self.pipeline_output_path = self.pipeline_parameters.get("pipeline_output_path")

        if not self.pipeline_output_path:
            raise ValueError("pipeline_output_path must be specified in the pipeline config under [pipeline]!")

        self.pipeline_output_prefix = self.pipeline_parameters.get("pipeline_output_prefix", self.pipeline_parameters.get("name").lower().replace(" ", "_"))

        if not self.pipeline_output_prefix:
            raise ValueError("pipeline_output_prefix must be specified in the pipeline config under [pipeline]!")

        base_path = (self.config_dir / self.pipeline_output_path).resolve()
        if self.pipeline_parameters["use_unique_path"]:
            self.pipeline_output_path = (base_path / f"{self.pipeline_output_prefix}_{self.run_id}")
        else:
            self.pipeline_output_path = (base_path / f"{self.pipeline_output_prefix}")

        self.pipeline_output_path.mkdir(exist_ok=True, parents=True)
        self.context["pipeline_output_path"] = self.pipeline_output_path

    def write_runtime_json(self):
        runtime = {
            "run_id":     self.run_id,
            "config_dir": str(self.config_dir),
            "git_commit": self.git_commit,
            "git_dirty":  self.git_dirty,
        }

        if getattr(self, "config_overrides", []):
            runtime["config_overrides"] = self.config_overrides

        if getattr(self, "source_run", None) is not None:
            runtime.update({
                "source_run": self.source_run,
                "source_run_id": self.source_run_id,
                "source_git_commit": self.source_git_commit,
                "source_git_dirty": self.source_git_dirty,
                "source_config_overrides": self.source_config_overrides,
                "reused_through_stage": self.reused_through_stage,
                "reused_file_count": self.reused_file_count,
            })

        with open(self.pipeline_output_path / "runtime.json", "w") as f:
            json.dump(runtime, f, indent=2)

    def reuse_outputs(self, source_run, through_stage_index):
        source_run = Path(source_run).expanduser().resolve()
        if not source_run.is_dir():
            raise RunItBackError(f"Source run not found: {source_run}")
        if source_run == self.pipeline_output_path:
            raise RunItBackError("Source run and new run cannot be the same directory")
        if through_stage_index <= 0 or through_stage_index > len(self.stages):
            raise RunItBackError(f"Invalid reuse boundary: stage {through_stage_index}")

        source_runtime_path = source_run / "runtime.json"
        if not source_runtime_path.is_file():
            raise RunItBackError(f"Source run is missing runtime.json: {source_run}")
        source_runtime = self.load_runtime_json(source_runtime_path)

        files_to_reuse = {}
        for stage in self.stages[:through_stage_index]:
            for pattern in stage.outputs_files:
                pattern_path = Path(os.path.expandvars(pattern)).expanduser()
                if pattern_path.is_absolute():
                    continue

                matches = [Path(filepath) for filepath in glob.glob(str(source_run / pattern_path)) if Path(filepath).is_file()]
                if not matches:
                    raise RunItBackError(f"Source run is missing output {pattern} from stage {stage.name}")

                for source_filepath in matches:
                    relative_path = source_filepath.relative_to(source_run)
                    files_to_reuse[relative_path] = source_filepath

        for relative_path in files_to_reuse:
            destination = self.pipeline_output_path / relative_path
            if destination.exists():
                raise RunItBackError(f"Reused output already exists in new run: {destination}")

        hardlinked_count = 0
        copied_count = 0
        for relative_path, source_filepath in files_to_reuse.items():
            destination = self.pipeline_output_path / relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(source_filepath, destination)
                hardlinked_count += 1
            except OSError:
                copy2(source_filepath, destination)
                copied_count += 1

        self.source_run = source_run.name
        self.source_run_id = source_runtime.get("run_id")
        self.source_git_commit = source_runtime.get("git_commit")
        self.source_git_dirty = source_runtime.get("git_dirty")
        self.source_config_overrides = source_runtime.get("config_overrides", [])
        self.reused_through_stage = through_stage_index
        self.reused_file_count = len(files_to_reuse)
        self.write_runtime_json()

        self.emit(f"-> Reused outputs through stage {through_stage_index} from: {source_run}")
        self.emit(f"-> Reused {len(files_to_reuse)} files ({hardlinked_count} hard linked, {copied_count} copied)")

    def configure_stages_path(self):
        config_dir = str(self.config_dir)
        if config_dir not in sys.path:
            sys.path.insert(0, config_dir)

        stages_path = self.pipeline_parameters.get("stages_path")
        if stages_path:
            stages_path = str((self.config_dir / stages_path).resolve())
            if stages_path not in sys.path:
                sys.path.insert(0, stages_path) # so we can reference/import functions within stages within other stages

    def make_context_stage(self):
        # this is kind of hacky, but this is correct for the context stage
        config = dict(self.context)
        config["filepath"] = self.resolve_stage_filepath(config["filepath"])
        config["context"] = self.context

        make_context_stage = Stage("make_context", config, self.pipeline_output_path)
        object_context = self._execute_stage(make_context_stage)
        self.context.update(object_context)

    def parse_stages(self):
        self.stages = []
        self.aux_stages = {}
        self.standalone_stages = []
        for i, key in enumerate(self.config["stages"].keys()):
            stage_config = self.config["stages"][key]
            stage_config["context"] = self.context # all stages get the context
            stage_config["filepath"] = self.resolve_stage_filepath(stage_config["filepath"])

            aux_stages = []
            for k in stage_config.get("aux_stages", []):
                aux_stage_config = self.config["aux_stages"][k]
                aux_stage_config["context"] = self.context # add context to aux stages as well
                aux_stage_config["filepath"] = self.resolve_stage_filepath(aux_stage_config["filepath"])
                aux_stages.append(Stage(k, aux_stage_config, self.pipeline_output_path, aux_stage_index=i))
            self.aux_stages[i] = aux_stages # stores all the auxillary stages for each stage

            self.stages.append(Stage(key, stage_config, self.pipeline_output_path))

        for i in self.config.get("standalone_stages", []):
            stage_config = self.config["standalone_stages"][i]
            stage_config["context"] = self.context # all stages get the context
            stage_config["filepath"] = self.resolve_stage_filepath(stage_config["filepath"])
            self.standalone_stages.append(Stage(i, stage_config, self.pipeline_output_path))

    def load_runtime_json(self, runtime_json_path):
        with open(runtime_json_path, "r") as f:
            runtime = json.load(f)
        return runtime

    @classmethod
    def load_run(cls, run_path, skip_validation=True, echo=False, config_dir=None):
        obj = cls.__new__(cls)  # allocate object without calling __init__
        obj._load_run(run_path, skip_validation=skip_validation, echo=echo, config_dir=config_dir)
        return obj

    def _load_run(self, run_path, skip_validation=True, echo=False, config_dir=None):
        run_path = Path(run_path).resolve()

        runtime = self.load_runtime_json(run_path / "runtime.json")

        tomls = list(run_path.glob("*.toml"))
        if len(tomls) != 1:
            raise RunItBackError(f"Expected exactly one .toml file in {run_path}, found {len(tomls)}")

        self.filepath = tomls[0]
        with open(self.filepath, "rb") as f:
            self.config = tomllib.load(f)
        self.config_overrides = runtime.get("config_overrides", [])
        self.config_override_values = apply_config_overrides(self.config, self.config_overrides)

        self.context               = self.config["context"]
        self.pipeline_parameters   = self.config["pipeline"]
        self.config_dir            = Path(runtime["config_dir"]) if config_dir is None else Path(config_dir).expanduser().resolve()
        self.context["config_dir"] = self.config_dir

        self.run_id                 = runtime["run_id"]
        self.git_commit             = runtime.get("git_commit")
        self.git_dirty              = runtime.get("git_dirty")
        self.source_run             = runtime.get("source_run")
        self.source_run_id          = runtime.get("source_run_id")
        self.source_git_commit      = runtime.get("source_git_commit")
        self.source_git_dirty       = runtime.get("source_git_dirty")
        self.source_config_overrides = runtime.get("source_config_overrides")
        self.reused_through_stage   = runtime.get("reused_through_stage")
        self.reused_file_count      = runtime.get("reused_file_count")

        # this replaces self.configure_pipeline_output() since we need the run_id to make the output path
        self.pipeline_output_prefix = self.pipeline_parameters.get("pipeline_output_prefix", self.pipeline_parameters.get("name").lower().replace(" ", "_"))
        self.pipeline_output_path = run_path
        self.context["pipeline_output_path"] = self.pipeline_output_path

        self.emit = Emitter(log=None, echo=echo)

        self.configure_stages_path()

        self.make_context_stage()

        self.parse_stages()

        if not skip_validation:
            self.validate()

    def init_logger(self):
        logging.basicConfig(filename=self.pipeline_output_path / self.filepath.with_suffix('.log').name,
                            format='%(asctime)s %(levelname)s: %(message)s',
                            filemode='w')
        log = logging.getLogger()
        log.setLevel(logging.INFO)
        return Emitter(log=log)

    def get_stage(self, name):
        for stage in self.stages:
            if stage.name == name:
                return stage
        raise ValueError(f"Stage with name '{name}' not found in pipeline!")

    def _execute_stage(self, stage):
        if self.emit.log is None:
            return stage.run_stage()

        stdout_tee = StdoutTee(sys.stdout, self.emit.log)
        try:
            with redirect_stdout(stdout_tee):
                return stage.run_stage()
        finally:
            stdout_tee.flush()

    def run(self, start_stage_index=0, end_stage_index=None, time_stages=False, run_aux_stage_index=None, run_standalone_stage=None):
        try:
            return self._run(start_stage_index=start_stage_index, end_stage_index=end_stage_index, time_stages=time_stages, run_aux_stage_index=run_aux_stage_index, run_standalone_stage=run_standalone_stage)
        except BaseException:
            self.emit.exception("-> Pipeline failed")
            raise

    def _run(self, start_stage_index=0, end_stage_index=None, time_stages=False, run_aux_stage_index=None, run_standalone_stage=None):

        if run_standalone_stage is not None:
            self.emit(f"-> Running standalone stage named {run_standalone_stage} only...")

            standalone_stage_to_run =  None
            for standalone_stage in self.standalone_stages:
                if standalone_stage.name == run_standalone_stage:
                    standalone_stage_to_run = standalone_stage

            if standalone_stage_to_run is None:
                raise RunItBackError(f"Standalone stage named \"{run_standalone_stage}\" not found in standalone_stages!")

            if time_stages:
                start_time = time.time()

            self._execute_stage(standalone_stage_to_run)

            if time_stages:
                elapsed = time.time() - start_time
                self.emit(f'-> Standalone stage {run_standalone_stage} completed in {format_duration(elapsed)}')

            return

        # run_aux_stage_index should correspond to the display stage index (not the zero indexed one)
        if run_aux_stage_index is not None:

            self.emit(f"-> Running auxillary stages for stage {run_aux_stage_index} only...")

            if time_stages:
                overall_start_time = time.time()

            for aux_stage in self.aux_stages[run_aux_stage_index]:
                if time_stages:
                    start_time = time.time()

                self._execute_stage(aux_stage)

                if time_stages:
                    elapsed = time.time() - start_time
                    self.emit(f'-> Stage {run_aux_stage_index} completed in {format_duration(elapsed)}')

            if time_stages:
                overall_elapsed = time.time() - overall_start_time
                self.emit(f'-> Pipeline completed in {format_duration(overall_elapsed)}')

            return

        if end_stage_index is not None and end_stage_index <= start_stage_index:
            raise ValueError(f"end_stage_index ({end_stage_index}) must be greater than start_stage_index ({start_stage_index})")

        selected = self.stages[start_stage_index:end_stage_index]

        self.emit(f"-> Running {len(selected)} stages...")

        for i,stage in enumerate(selected):
            self.print_stage(stage, i+start_stage_index)

        if start_stage_index > 0:
            self.emit(f"-> Starting pipeline from: stage {start_stage_index+1} -> {self.stages[start_stage_index].name}")

        if end_stage_index is not None:
            self.emit(f"-> Stopping pipeline at:   stage {end_stage_index} -> {self.stages[end_stage_index-1].name}")

        if time_stages:
            overall_start_time = time.time()

        for i,stage in enumerate(selected):

            stage_index = i+start_stage_index

            if time_stages:
                start_time = time.time()

            self.emit(f"-> Running stage: {stage.name}")
            self.emit()
            self.emit("="*64)
            self.emit()

            output = self._execute_stage(stage)
            if self.aux_stages[i+start_stage_index] != []:
                for aux_stage in self.aux_stages[i+start_stage_index]:
                    self.emit(f"-> Running aux stage: {aux_stage.name}")
                    self._execute_stage(aux_stage)

            if stage_index < len(self.stages) - 1:
                if output is not None:
                    if isinstance(output, tuple):
                        self.stages[stage_index+1].inputs = output
                    else:
                        self.stages[stage_index+1].inputs = (output,)

            if time_stages:
                elapsed = time.time() - start_time
                self.emit(f'-> Stage {stage_index+1} completed in {format_duration(elapsed)}')

        if time_stages:
            overall_elapsed = time.time() - overall_start_time
            self.emit(f'-> Pipeline completed in {format_duration(overall_elapsed)}')

        self.emit("-> Done!")

    # this wont always work, only works if stages are independent of one another
    def run_stage(self, index):
        try:
            return self._execute_stage(self.stages[index])
        except BaseException:
            self.emit.exception(f"-> Stage {self.stages[index].name} failed")
            raise

    def print_stage(self, stage, index):
        input_files_str  = "\n            ".join(stage.inputs_files)
        output_files_str = "\n            ".join(stage.outputs_files)
        self.emit(f"""
[Stage {index+1:02d}]
name    : {stage.name}
func    : {stage.func_name}
params  : {stage.params}
inputs
    types  : {stage.inputs_type_str}
    files  : {input_files_str}
outputs
    types  : {stage.outputs_type_str}
    files  : {output_files_str}
aux stages : {[aux_stage.name for aux_stage in self.aux_stages[index]]}
""")

        """
        self.emit(f'-- Stage {index+1:02d} --')
        self.emit(f'name   -> {stage.name}')
        self.emit(f'func   -> {stage.func_name}')
        self.emit(f'params -> {stage.params}')
        self.emit(f'input  types -> {stage.inputs_type_str}')
        self.emit(f'output types -> {stage.outputs_type_str}')
        self.emit(f'input  files -> {stage.inputs_files}')
        self.emit(f'output files -> {stage.outputs_files}')
        self.emit()
        """

    def resolve_stage_filepath(self, filepath):
        filepath = Path(filepath)
        if filepath.is_absolute():
            return filepath
        return (self.config_dir / filepath).resolve()

    def validate(self):

        # string type checking (not the most robust, but it works well for simple cases and is a good start)
        for i,stage in enumerate(self.stages):

            contents = stage.filepath.read_text()
            tree = ast.parse(contents)

            node = ast_get_function_by_name(tree, stage.func_name)
            if node is None:
                raise RunItBackError(f"Function {stage.func_name} not found in stage {stage.filepath}!")

            n_defaults = len(node.args.defaults)
            for arg in node.args.args[:len(node.args.args)-n_defaults]:
                stage.inputs_type_str.append(ast.unparse(arg.annotation))

            # return can be tuple, single type, or None
            if getattr(node, 'returns', None) is not None:
                rtypes = get_output_type_strings(node)
                if rtypes == ['None']:
                    stage.outputs_type_str = []
                else:
                    stage.outputs_type_str = rtypes

        '''
        for i in range(len(self.stages)-1):
            curr_stage = self.stages[i]
            next_stage = self.stages[i+1]

            if curr_stage.outputs_type_str != next_stage.inputs_type_str:
                raise RunItBackError(f"Type mismatch between stage '{curr_stage.name}' outputs {curr_stage.outputs_type_str} and stage '{next_stage.name}' inputs {next_stage.inputs_type_str}!")

        # check the number of args between stages

        for i in range(len(self.stages)-1):
            curr_stage = self.stages[i]
            next_stage = self.stages[i+1]

            curr_contents = curr_stage.filepath.read_text()
            curr_tree = ast.parse(curr_contents)

            next_contents = next_stage.filepath.read_text()
            next_tree = ast.parse(next_contents)

            n_outputs = 1
            node = ast_get_function_by_name(curr_tree, curr_stage.func_name)
            n_outputs = get_number_of_output_args(node)

            node = ast_get_function_by_name(next_tree, next_stage.func_name)
            n_required_args = get_number_of_input_args(node)

            if n_outputs != n_required_args:
                raise RunItBackError(f"Number of outputs from stage '{curr_stage.name}' ({n_outputs}) does not match number of required inputs for stage '{next_stage.name}' ({n_required_args})!")

        for stage in self.stages:
            contents = stage.filepath.read_text()
            tree = ast.parse(contents)

            node = ast_get_function_by_name(tree, stage.func_name)

            n_outputs = get_number_of_output_args(node)
            n_required_args = get_number_of_input_args(node)

            if n_outputs != len(stage.outputs_config):
                raise RunItBackError(f"Number of function outputs from stage '{stage.name}' ({n_outputs}) does not match number of outputs in config ({len(stage.outputs_config)})!")

            if n_required_args != len(stage.inputs_config):
                raise RunItBackError(f"Number of function inputs from stage '{stage.name}' ({n_required_args}) does not match number of inputs in config ({len(stage.inputs_config)})!")
        '''

def get_number_of_input_args(node):
    if node.args.posonlyargs != []:
        raise ValueError("RIB does not support stages with positional-only args yet!")

    if node.args.vararg is not None:
        raise ValueError("RIB does not support stages with *args yet!")

    if node.args.kwonlyargs != []:
        raise ValueError("RIB does not support stages with keyword-only args yet!")

    if node.args.kw_defaults != []:
        raise ValueError("RIB does not support stages with keyword-only args yet!")

    if node.args.kwarg is not None:
        raise ValueError("RIB does not support stages with **kwargs yet!")

    for arg in node.args.args:
        if arg.annotation is None:
            raise RunItBackError(f"RIB requires all stage function arguments to be annotated with types, missing annotation for argument '{arg.arg}' in function '{node.name}'")

    # can make this more robust eventually

    n_args = len(node.args.args)
    n_defaults = len(node.args.defaults)
    n_required_args = n_args - n_defaults

    return n_required_args

def get_number_of_output_args(node):
    n_outputs = 0 # always at least 1 for None return
    for n in ast.walk(node):

        # skip nested functions
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            break

        if isinstance(n, ast.Return):
            if n.value is None:
                n_outputs = 0
            elif isinstance(n.value, ast.Tuple):
                n_outputs = len(n.value.elts)
            else:
                n_outputs = 1

    return n_outputs

def ast_get_function_by_name(tree, func_name):
    for node in tree.body:
        if type(node) is ast.FunctionDef and node.name == func_name:
            return node

def is_file(fn):
    return "." in fn

def expand_file_templates(patterns, context, stage_name, pattern_type):
    expanded = []
    for pattern in patterns:
        try:
            expanded.append(Template(pattern).substitute(context))
        except KeyError as error:
            key = error.args[0]
            raise RunItBackError(f"Missing context key '{key}' while expanding {pattern_type} pattern '{pattern}' for stage '{stage_name}'") from error
        except ValueError as error:
            raise RunItBackError(f"Invalid template in {pattern_type} pattern '{pattern}' for stage '{stage_name}': {error}") from error
    return expanded

def check_files_exist(patterns, relative_to):
    missing = []
    for pat in patterns:

        abspath_patt = Path(os.path.expandvars(os.path.expanduser(str(pat))))
        if not abspath_patt.is_absolute():
            abspath_patt = (relative_to / abspath_patt).resolve()

        # glob pattern
        if "*" in str(abspath_patt):
            matches = glob.glob(str(abspath_patt))
            if not matches:
                missing.append(abspath_patt)
        else:
            if not abspath_patt.exists():
                missing.append(abspath_patt)

    return missing

def has_kwarg(func, kwarg_name: str) -> bool:
    sig = inspect.signature(func)
    p = sig.parameters.get(kwarg_name)
    if p is None:
        return False
    return p.kind in (
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    )

def get_output_type_strings(node: ast.FunctionDef) -> list[str]:
    r = node.returns

    # no annotation => implicitly None
    if r is None:
        return ["None"]

    # explicit "-> None"
    if isinstance(r, ast.Constant) and r.value is None:
        return ["None"]

    # support "-> (A, B)" style if you use it
    if isinstance(r, ast.Tuple):
        return [ast.unparse(el) for el in r.elts]

    # single annotated output
    return [ast.unparse(r)]

def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f}s"

    minutes, sec = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {sec:.2f}s"

    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {sec:.2f}s"

class Emitter:
    def __init__(self, log=None, echo: bool = True):
        self.log = log
        self.echo = echo

    def __call__(self, msg: str = '', level: str = "info") -> None:
        if self.echo:
            print(msg)
        if self.log is not None:
            log_fn = getattr(self.log, level, self.log.info)
            log_fn(msg)

    def info(self, msg: str) -> None:
        self(msg, "info")

    def warning(self, msg: str) -> None:
        self(msg, "warning")

    def error(self, msg: str) -> None:
        self(msg, "error")

    def exception(self, msg: str) -> None:
        self(msg, "exception")

class StdoutTee:
    def __init__(self, stream, log):
        self.stream = stream
        self.log = log
        self.buffer = ""

    def write(self, text):
        written = self.stream.write(text)
        self.stream.flush()
        self.buffer += text
        lines = self.buffer.split("\n")
        self.buffer = lines.pop()
        for line in lines:
            self.log.info(line.rstrip("\r"))
        return written

    def flush(self):
        self.stream.flush()
        if self.buffer:
            self.log.info(self.buffer.rstrip("\r"))
            self.buffer = ""

    def __getattr__(self, name):
        return getattr(self.stream, name)

def print_run_it_back():
    print(
r"""
█▀█ █ █ █▀▄ ▀█▀ ▀█▀ █▄▄ ▄▀█ █▀▀ █▄▀
█   █▄█ █ █ ▄█▄  █  █▄█ █▀█ █▄▄ █ █
"""
    )
