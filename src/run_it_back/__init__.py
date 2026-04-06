__version__ = "0.1.0"

import ast
from pathlib import Path
import importlib.util
import tomllib
import glob

SAVE_FUNC_NAMES = ("save", "savefig", "to_csv", "to_parquet", "to_hdf",
                   "to_excel", "to_json", "to_pickle", "to_feather",
                   "writeto", "write_to", "write", "savetxt", "savez",
                   "savez_compressed", "imwrite", "dump")

def load_pipeline(filepath: Path):
    with open(filepath, "rb") as f:
        config = tomllib.load(f)

    return Pipeline(config)  # for now, just return the dict

class RunItBackError(Exception):
    pass

class Stage:
    def __init__(self, name, config):

        self.name = name

        self.filepath = config.get("filepath", None)

        if self.filepath is not None:
            self.filepath = Path(self.filepath)
        else:
            self.filepath = Path('steps') / Path(name).with_suffix('.py')

        self.func_name = config.get("func_name", name)

        self.params = config.get("params", None)

        self.inputs = None
        self.outputs = None

        self.inputs_config = config.get("inputs", None)
        self.outputs_config = config.get("outputs", None)

        self.inputs_files = [el for el in self.inputs_config if is_file(el)]
        self.outputs_files = [el for el in self.outputs_config if is_file(el)]

        self.inputs_type_str = []
        self.outputs_type_str = []

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
    def __init__(self, config):

        self.config = config

        self.context = config["context"] # global dict

        self.stages = []

        for key in config["stage"].keys():
            st = config["stage"][key]
            self.stages.append(Stage(key, st))

    def run_all(self):
        for i,stage in enumerate(self.stages):

            print(f"Running stage: {stage.name}")
            output = stage.run_stage()

            if i < len(self.stages) - 1:
                if isinstance(output, tuple):
                    self.stages[i+1].inputs = output
                else:
                    self.stages[i+1].inputs = (output,)

    def validate(self):

        # string type checking (not the most robust, but it works well for simple cases and is a good start)
        for stage in self.stages:

            contents = stage.filepath.read_text()
            tree = ast.parse(contents)

            node = ast_get_function_by_name(tree, stage.func_name)

            for arg in node.args.args:
                stage.inputs_type_str.append(ast.unparse(arg.annotation))

            if isinstance(node.returns, ast.Tuple):
                for ret in node.returns.elts:
                    stage.outputs_type_str.append(ast.unparse(ret))
            else:
                stage.outputs_type_str.append(ast.unparse(node.returns))

            print(stage.func_name)
            print('input  ->', stage.inputs_type_str)
            print('output ->', stage.outputs_type_str)
            print()

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
            raise RunItBackError(f"RIB requires all stage function arguments to be annotated with types, missing annotation for argument '{arg.arg}' in function '{self.func_name}' in stage '{self.filepath}'")

    # can make this more robust eventually

    n_args = len(node.args.args)
    n_defaults = len(node.args.defaults)
    n_required_args = n_args - n_defaults

    return n_required_args

def get_number_of_output_args(node):
    n_outputs = 1 # always at least 1 for None return
    for n in ast.walk(node):
        if isinstance(n, ast.Return):
            if isinstance(n.value, ast.Tuple):
                n_outputs = len(n.value.elts)

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

def print_dag(pipeline):
    """Print an ASCII DAG of the pipeline."""
    stages = pipeline.stages

    # build adjacency
    all_outputs = {}
    for stage in stages:
        for out in stage.outputs_config:
            all_outputs[out] = stage.name

    edges = {}
    parents = {}
    for stage in stages:
        edges[stage.name] = []
        parents[stage.name] = []

    for stage in stages:
        for inp in stage.inputs_config:
            if inp in all_outputs:
                parent = all_outputs[inp]
                if parent != stage.name:
                    edges[parent].append(stage.name)
                    parents[stage.name].append(parent)

    # find roots
    roots = [s.name for s in stages if not parents[s.name]]

    # BFS to assign depths
    depth = {}
    queue = [(r, 0) for r in roots]
    while queue:
        name, d = queue.pop(0)
        if name in depth:
            depth[name] = max(depth[name], d)
        else:
            depth[name] = d
        for child in edges[name]:
            queue.append((child, d + 1))

    # group by depth
    layers = {}
    for name, d in depth.items():
        layers.setdefault(d, []).append(name)

    # print
    for d in sorted(layers.keys()):
        for name in layers[d]:
            indent = "    " * d
            stage = next(s for s in stages if s.name == name)

            mem_in = [i for i in stage.inputs_config if not is_file(i)]
            mem_out = [o for o in stage.outputs_config if not is_file(o)]
            file_out = [o for o in stage.outputs_config if is_file(o)]

            if d == 0:
                prefix = "[*] "
            else:
                prefix = "+-- "

            line = f"{indent}{prefix}{name}"

            tags = []
            if mem_in:
                tags.append(f"in: {', '.join(mem_in)}")
            if mem_out:
                tags.append(f"out: {', '.join(mem_out)}")
            if file_out:
                tags.append(f"saves: {', '.join(file_out)}")

            if tags:
                line += f"  ({' | '.join(tags)})"

            print(line)

            if edges[name]:
                print(f"{indent}    |")
