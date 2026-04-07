import argparse
from pathlib import Path
from run_it_back import load_pipeline, print_dag

def main():
    parser = argparse.ArgumentParser(
        prog="rib",
        description="run-it-back: reproducible data analysis pipelines",
    )

    parser.add_argument("command", choices=["run", "validate", "dag"])

    parser.add_argument(
        "file",
        help="path to pipeline file (default: pipeline.toml)",
    )

    parser.add_argument(
        "--skip-validation", "-s",
        action="store_true",
        help="skip validation of pipeline file and steps (default: False)",
    )

    parser.add_argument("stage", nargs="?", type=int, help="Optional stage index (0-based)")

    args = parser.parse_args()
    pipeline_file = Path(args.file)

    pipeline = load_pipeline(pipeline_file)

    if not args.skip_validation:
        pipeline.validate()

    if args.command == "run" and args.stage is None:
        pipeline.run_all()
    elif args.command == "run" and args.stage is not None:
        pipeline.run_stage(args.stage)

    print_dag(pipeline)

if __name__ == "__main__":
    main()
