from setuptools import setup, find_packages
import os
from packaging.requirements import Requirement

# TODO: move package root up a level and let vra_experiments install as etl_pipeline.logger (for example)


def parse_requirements(filename, version_spec=True):
    """list reqs from requirements.txt
    Optionally stripping the version spec"""
    reqs = []
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if version_spec:
                reqs.append(line)
            else:
                req = Requirement(line)
                reqs.append(req.name)
    return reqs


requirements_path = os.path.join(os.path.dirname(__file__), "requirements.txt")
install_requires = parse_requirements(requirements_path)

setup(
    name="etl-pipeline",
    packages=find_packages(),
    py_modules=[
        "load_env",
        "logger",
    ],  # TODO: any way to mark in the file that we want these non-package modules discovered.
    install_requires=install_requires,
)
