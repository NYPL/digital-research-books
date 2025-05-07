import sys

from counter_5_controller import Counter5Controller
from datetime import datetime
from main import load_env_file


def main():
    load_env_file("local-qa", file_string="config/local-qa.yaml")

    if len(sys.argv) <= 1:
        print(
            f"No reporting period passed in. Generating report for Jan {datetime.now().year}!"
        )

    counter_5_controller = (
        Counter5Controller(None)
        if (len(sys.argv) <= 1)
        else Counter5Controller(sys.argv[1:])
    )
    counter_5_controller.create_reports()


if __name__ == "__main__":
    main()
