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
