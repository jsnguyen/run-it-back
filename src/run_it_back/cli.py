import argparse
from pathlib import Path
from run_it_back import load_pipeline, print_dag

def main():
    parser = argparse.ArgumentParser(
        prog="rib",
        description="run-it-back: reproducible data analysis pipelines",
    )

    parser.add_argument(
        "--file", "-f",
        default="pipeline.toml",
        help="path to pipeline file (default: pipeline.toml)",
    )

    parser.add_argument(
        "--skip-validation", "-s",
        action="store_true",
        help="skip validation of pipeline file and steps (default: False)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run the pipeline")
    run_parser.add_argument("step", nargs="?", help="run a single step")

    args = parser.parse_args()
    pipeline_file = Path(args.file)


    if args.command == "run":
        pipeline = load_pipeline(pipeline_file)

        if not args.skip_validation:
            pipeline.validate()

        pipeline.run_all()

    print_dag(pipeline)


if __name__ == "__main__":
    main()
