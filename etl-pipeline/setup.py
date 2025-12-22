from setuptools import setup, find_packages
import os
from packaging.requirements import Requirement


def parse_requirements(filename):
    """list requirements.txt, package name only"""
    reqs = []
    with open(filename, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
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
    ],  # TODO: any way to mark in the file that we want these discovered.
    install_requires=install_requires,
)
