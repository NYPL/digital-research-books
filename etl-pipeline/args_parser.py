import argparse


def env_parser():
    env_parser = argparse.ArgumentParser(add_help=False)
    env_parser.add_argument(
        "-e",
        "--environment",
        required=True,
        help="Environment for deployment, sets env file to load",
    )

    return env_parser


def create_arg_parser():
    parser = argparse.ArgumentParser(
        description="Runs etl-pipeline scripts and processes", parents=[env_parser()]
    )

    parser.add_argument("-p", "--process", help="The name of the process job to be run")
    parser.add_argument("-sc", "--script", help="The name of the script to run")
    parser.add_argument(
        "-i",
        "--ingestType",
        help="The interval to run the ingest over. Generally daily/complete/custom",
    )
    parser.add_argument(
        "-f",
        "--inputFile",
        help="Name of file to ingest. Ignored if -i custom is not set",
    )
    parser.add_argument(
        "-s", "--startDate", help="Start point for coverage period to query/process"
    )
    parser.add_argument(
        "-l",
        "--limit",
        help="Set overall limit for number of records imported in this process",
    )
    parser.add_argument(
        "-o",
        "--offset",
        help="Set start offset for current processed (for batched import process)",
    )
    parser.add_argument(
        "-r",
        "--singleRecord",
        help="Single record ID for ingesting an individual record",
    )
    parser.add_argument(
        "-src", "--source", help="Run a process against records from a specified source"
    )
    parser.add_argument("options", nargs="*", help="Additional arguments")

    return parser
