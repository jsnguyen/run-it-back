import shlex
import sys
import argparse
from pathlib import Path
from run_it_back import Pipeline

def parse_stage_range(value):
    if value is None:
        return 0, None

    if ":" not in value:
        stage = int(value)
        return stage - 1, stage

    start, end = value.split(":", maxsplit=1)
    return int(start) - 1 if start else 0, int(end) if end else None

def main():

    parser = argparse.ArgumentParser(
        prog="rib",
        description="run-it-back: reproducible data analysis pipelines",
    )

    parser.add_argument("file", help="path to .toml pipeline file")
    parser.add_argument("--skip-validation", "-s", action="store_true", help="skip validation of pipeline file and steps (default: False)")
    parser.add_argument("--time-stages", "-t", action="store_true", help="print execution time for each stage")
    parser.add_argument("--stages", help="1-indexed stage selection: N, N:, :N, or N:M")
    parser.add_argument("--run-aux", type=int, default=None, help="Run only the auxillary part of stage N")

    args = parser.parse_args()
    pipeline_file = Path(args.file)

    pipeline = Pipeline(pipeline_file, skip_validation=args.skip_validation)
    command_line = shlex.join(sys.argv)
    pipeline.emit(f'-> Command: {command_line}')

    start, end = parse_stage_range(args.stages)

    pipeline.run(
        start_stage_index=start,
        end_stage_index=end,
        time_stages=args.time_stages,
        run_aux_stage_index=args.run_aux,
    )

if __name__ == "__main__":
    main()
