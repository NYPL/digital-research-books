from pathlib import Path
import json
import re


def get_constants():
    constants = {}

    constants_dir = Path(__file__).parent
    for path in constants_dir.glob("*.json"):
        file_name = re.search(r"[\\/]+([a-z0-9]+)\.json", str(path)).group(1)

        with open(path, "r") as constants_file:
            file_constants = json.load(constants_file)

            constants[file_name] = file_constants

    return constants
