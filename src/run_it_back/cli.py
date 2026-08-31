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
    parser.add_argument("--override", action="append", default=[], metavar="KEY=VALUE", help="override an existing TOML value; may be repeated")
    parser.add_argument("--from-run", help="reuse outputs from stages before the selected start stage")
    parser.add_argument("--run-aux-stage", type=int, default=None, help="Run only the auxillary part of stage N")
    parser.add_argument("--run-standalone-stage", type=str, default=None, help="Run only the named standalone stage")

    args = parser.parse_args()
    pipeline_file = Path(args.file)
    start, end = parse_stage_range(args.stages)

    if args.from_run is not None and start == 0:
        parser.error("--from-run requires --stages to start after stage 1")
    if args.from_run is not None and (args.run_aux_stage is not None or args.run_standalone_stage is not None):
        parser.error("--from-run cannot be combined with auxiliary or standalone stages")

    pipeline = Pipeline(pipeline_file, skip_validation=args.skip_validation, overrides=args.override)
    command_line = shlex.join(sys.argv)
    pipeline.emit(f'-> Command: {command_line}')

    if args.from_run is not None:
        try:
            pipeline.reuse_outputs(args.from_run, through_stage_index=start)
        except BaseException:
            pipeline.emit.exception("-> Reusing outputs failed")
            raise

    pipeline.run(
        start_stage_index=start,
        end_stage_index=end,
        time_stages=args.time_stages,
        run_aux_stage_index=args.run_aux_stage,
        run_standalone_stage=args.run_standalone_stage,
    )

if __name__ == "__main__":
    main()
