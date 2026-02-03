from typing import Iterator


def chunk(xs: Iterator, size: int) -> Iterator[list]:
    """yield elements in batches (similar to `utils.batched()` )"""
    while True:
        chunk = []
        try:
            for _ in range(size):
                chunk.append(next(xs))

            yield chunk
        except StopIteration:
            if chunk:
                yield chunk

            break
