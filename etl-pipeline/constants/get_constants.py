from glob import glob
import json
import re


def get_constants():
    constants = {}

    for path in glob("./constants/*.json"):
        file_name = re.search(r"\/([a-z0-9]+)\.json", path).group(1)

        with open(path, "r") as constants_file:
            file_constants = json.load(constants_file)

            constants[file_name] = file_constants

    return constants
