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


def require_env(var):
    """Get required environment variable with clear error."""
    value = os.environ.get(var)
    if value is None:
        raise ValueError(f'Environment variable "{var}" must be available.')
    return value


# FUN ALT (slightly over-engineered)
# _REQUIRED = object()
# def read_env(var, default=_REQUIRED):
#     """Read env var with an informative error.

#     Args:
#         var: Environment variable name
#         default: Default value if not set. Omit to require the variable.

#     Returns:
#         str: The environment variable value or default

#     Raises:
#         ValueError: If variable is required but not set
#         TypeError: If default is not str or None
#     """
#     value = os.environ.get(var)
#     if value is not None:
#         return value

#     if default is _REQUIRED:
#         raise ValueError(f'Environment variable "{var}" must be available.')

#     if not isinstance(default, (str, type(None))):
#         raise TypeError(f'default must be str or None, got {type(default).__name__}')

#     return default


def wrap(s, max_width=80):
    wrapper = textwrap.TextWrapper(
        width=max_width, tabsize=4, replace_whitespace=False, drop_whitespace=True
    )
    return "\n".join([wrapper.fill(l) for l in s.splitlines()])
