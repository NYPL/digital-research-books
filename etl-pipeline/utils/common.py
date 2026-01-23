import textwrap
import os


def batched(generator, batch_size):
    """
    A generator function that yields batches from a generator of unknown length.
    """
    batch = []
    for item in generator:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


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
