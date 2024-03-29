import nox
from nox import options

import toml

options.sessions = ["format", "mypy", "test"]


def get_project_deps():
    parsed = toml.load("pyproject.toml")
    deps = parsed["project"]["dependencies"]

    return deps


@nox.session
def mypy(session):
    session.install("mypy", *get_project_deps())
    session.run("mypy", ".")


@nox.session
def format(session):
    session.install("isort")
    session.run("isort", "saru")


@nox.session
def test(session):
    session.install("pytest", *get_project_deps())
    session.install("-e", ".")
    session.run("pytest")


@nox.session(reuse_venv=True)
def docs(session):
    session.install("pdoc3", *get_project_deps())
    session.run("pdoc", "--html", "saru", "--force", "-o", "docs")
