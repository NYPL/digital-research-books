import textwrap
import os


def read_env(var, require=True):
    """Read env var with an informative error.

    Optionally return None if var does not exist.
    """
    try:
        return os.environ[var]
    except KeyError:
        if require:
            raise ValueError(f'Environment variable "{var}" must be available.')


def wrap(s, max_width=80):
    wrapper = textwrap.TextWrapper(
        width=max_width, tabsize=4, replace_whitespace=False, drop_whitespace=True
    )
    return "\n".join([wrapper.fill(l) for l in s.splitlines()])
