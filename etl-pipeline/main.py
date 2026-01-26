import os
import newrelic.agent

import inspect

from args_parser import create_arg_parser
from utils.load_env import load_env
from logger import configure_loggers, create_log


if os.environ.get("NEW_RELIC_LICENSE_KEY", None):
    newrelic.agent.initialize(
        config_file="newrelic.ini", environment=os.environ.get("ENVIRONMENT", "local")
    )


def main(args):
    logger = create_log(__name__)

    environment = args.environment

    if args.script is not None:
        logger.info(f"Running script {args.script} in {environment}")
        run_script(args.script, *args.options)

        return

    process = args.process
    process_type = args.ingestType
    custom_file = args.inputFile
    start_date = args.startDate
    single_record = args.singleRecord
    limit = args.limit
    offset = args.offset
    source = args.source
    options = args.options

    logger.info(f"Starting process {process} in {environment}")

    available_processes = register_processes()

    try:
        process_class = available_processes[process]

        process_instance = process_class(
            process_type,
            custom_file,
            start_date,
            single_record,
            limit,
            offset,
            source,
            options,
        )
    except Exception:
        logger.exception(f"Failed to initialize process {process} in {environment}")
        return

    if process in ("APIProcess", "DevelopmentSetupProcess", "MigrationProcess"):
        process_instance.run()
    else:
        app = newrelic.agent.register_application(timeout=10.0)

        with newrelic.agent.BackgroundTask(app, name=process_instance):
            process_instance.run()


def register_processes():
    import processes

    process_classes = inspect.getmembers(processes, inspect.isclass)

    return dict(process_classes)


def register_scripts() -> dict:
    import scripts

    script_functions = inspect.getmembers(scripts, inspect.isfunction)

    return dict(script_functions)


def run_script(script: str, *args):
    scripts = register_scripts()

    script = scripts.get(script)

    if script is not None:
        script(*args)


if __name__ == "__main__":
    parser = create_arg_parser()
    args = parser.parse_args()

    load_env(f"./config/.env.{args.environment}")
    if os.environ.get("ENVIRONMENT") is None:
        # MAYBE: remove  this...
        os.environ["ENVIRONMENT"] = args.environment
    configure_loggers()

    main(args)
    newrelic.agent.shutdown_agent()
