__version__ = "0.1.0"

import ast
import time
from pathlib import Path
import importlib.util
import inspect
import tomllib
import glob

SAVE_FUNC_NAMES = ("save", "savefig", "to_csv", "to_parquet", "to_hdf",
                   "to_excel", "to_json", "to_pickle", "to_feather",
                   "writeto", "write_to", "write", "savetxt", "savez",
                   "savez_compressed", "imwrite", "dump")

class RunItBackError(Exception):
    pass

class Stage:
    def __init__(self, name, config={}):

        self.name = name

        self.filepath = config.get("filepath", None)

        if self.filepath is not None:
            self.filepath = Path(self.filepath)
        else:
            self.filepath = Path(name).with_suffix('.py')

        self.func_name = config.get("func_name", name)

        self.params = config.get("params", None)

        self.inputs = None
        self.outputs = None

        self.inputs_config = config.get("inputs", [])
        self.outputs_config = config.get("outputs", [])

        self.inputs_files = [el for el in self.inputs_config if is_file(el)]
        self.outputs_files = [el for el in self.outputs_config if is_file(el)]

        self.inputs_type_str = []
        self.outputs_type_str = []

        self.context = config.get("context", {})

    def __repr__(self):
        return f"Stage(name={self.name}, run={self.filepath}, func_name={self.func_name}, params={self.params}, path={self.filepath})"

    def run_stage(self):
        if self.func_name is None:
            raise ValueError(f"Stage {self.name} is missing 'func_name' in config")

        missing_files = check_files_exist(self.inputs_files)
        if missing_files != []:
            raise RunItBackError(f"Missing input files! {missing_files}")

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

        missing_files = check_files_exist(self.outputs_files)
        if missing_files != []:
            raise RunItBackError(f"Missing output files! {missing_files}")

        return res

class Pipeline:

    def __init__(self, filepath, log=None):

        with open(filepath, "rb") as f:
            self.config = tomllib.load(f)

        # context is the global state that gets passed to all stages
        # use the object context stage to initialize python objects
        self.context = self.config["context"] # initialize as config dict
        self.make_object_context() # this is so that we can also initialize objects in memory, not just simple types from the toml file

        if log is not None:
            self.emit = Emitter(log=log)
            self.context["log"] = log

        self.stages = []

        for key in self.config["stages"].keys():
            st = self.config["stages"][key] | {"context": self.context} # all stages get the context
            self.stages.append(Stage(key, st))

    def get_stage(self, name):
        for stage in self.stages:
            if stage.name == name:
                return stage
        raise ValueError(f"Stage with name '{name}' not found in pipeline!")

    def make_object_context(self):
        # run with the filepath to the context making file
        # also pass in the existing dict that we have so far, this will be updated
        context_stage = Stage(Path(self.context["filepath"]).stem, {"filepath": self.context["filepath"], "context": self.context}) # kind of a weird call here, but it works
        self.context = self.context | context_stage.run_stage() # merge into new contexts, might eventually have to do conflict checking here? right now context_stage just overrides

    def run_all(self, start_stage_index=0, time_stages=False):

        self.emit("Starting pipeline execution...")
        self.emit(f"Running {len(self.stages)-start_stage_index} stages...")

        if start_stage_index > 0:
            self.emit(f"Starting pipeline from stage index {start_stage_index} -> {self.stages[start_stage_index].name}")

        if time_stages:
            overall_start_time = time.time()

        for i,stage in enumerate(self.stages[start_stage_index:]):

            stage_index = i+start_stage_index

            if time_stages:
                start_time = time.time()
            self.emit(f"Running stage -> {stage.name}")
            output = stage.run_stage()

            if i < len(self.stages) - 1:
                if output is not None:
                    if isinstance(output, tuple):
                        self.stages[stage_index+1].inputs = output
                    else:
                        self.stages[stage_index+1].inputs = (output,)

            if time_stages:
                elapsed = time.time() - start_time
                self.emit(f'Stage {stage_index} completed in {format_duration(elapsed)}')

        if time_stages:
            overall_elapsed = time.time() - overall_start_time
            self.emit(f'Pipeline completed in {format_duration(overall_elapsed)}')

        self.emit("Done!")

    # this wont always work, only works if stages are independent of one another
    def run_stage(self, index):
        output = self.stages[index].run_stage()

    def validate(self):

        # string type checking (not the most robust, but it works well for simple cases and is a good start)
        for i,stage in enumerate(self.stages):

            contents = stage.filepath.read_text()
            tree = ast.parse(contents)

            node = ast_get_function_by_name(tree, stage.func_name)

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

            self.emit(f'-- Stage {i} --')
            self.emit(f'name   -> {stage.name}')
            self.emit(f'func   -> {stage.func_name}')
            self.emit(f'params -> {stage.params}')
            self.emit(f'input  -> {stage.inputs_type_str}')
            self.emit(f'output -> {stage.outputs_type_str}')
            self.emit()

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

def check_files_exist(patterns):
    missing = []
    for pat in patterns:
        # glob pattern
        if "*" in pat:
            matches = glob.glob(pat)
            if not matches:
                missing.append(pat)
        else:
            if not Path(pat).exists():
                missing.append(pat)

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
