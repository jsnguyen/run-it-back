import argparse
from pathlib import Path
from run_it_back import Pipeline
import logging

def main():
    parser = argparse.ArgumentParser(
        prog="rib",
        description="run-it-back: reproducible data analysis pipelines",
    )

    parser.add_argument("command", choices=["run", "runfrom"])
    parser.add_argument("file", help="path to .toml pipeline file")
    parser.add_argument("--skip-validation", "-s", action="store_true", help="skip validation of pipeline file and steps (default: False)")
    parser.add_argument("--time-stages", "-t", action="store_true", help="print execution time for each stage")
    parser.add_argument("--stage", nargs="?", type=int, help="Optional stage index (0-based)")

    args = parser.parse_args()
    pipeline_file = Path(args.file)

    logging.basicConfig(filename=pipeline_file.with_suffix('.log'),
                        format='%(asctime)s %(levelname)s: %(message)s',
                        filemode='w')
    log = logging.getLogger()
    log.setLevel(logging.INFO)

    pipeline = Pipeline(pipeline_file, log=log)

    if not args.skip_validation:
        pipeline.validate()

    if args.command == "run" and args.stage is None:
        pipeline.run_all(time_stages=args.time_stages)
    elif args.command == "run" and args.stage is not None:
        pipeline.run_stage(args.stage)
    elif args.command == "runfrom":
        if args.stage is None:
            err = "Error: --stage argument is required for 'runfrom' command"
            log.error(err)
            raise ValueError(err)
        pipeline.run_all(start_stage_index=args.stage, time_stages=args.time_stages)

if __name__ == "__main__":
    main()
