import os


def read_env(var):
    """Read env var with an informative error"""
    if var not in os.environ:
        raise ValueError(f'Environment variable "{var}" not available.')
    return os.environ[var]


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
