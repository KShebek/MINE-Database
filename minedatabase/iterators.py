from collections.abc import Iterator
from threading import BoundedSemaphore
from typing import Iterable, Iterator, TypeVar
from itertools import islice, chain

T = TypeVar('T')

class BoundedChunks(Iterator):
    """Limits the number of chunks to yield until yielded values are
    acknowledged.
    """

    def __init__(self, bound: int, it: Iterable, chunk_size: int = 1):
        self._it = iter(it)
        self._sem = BoundedSemaphore(bound)
        self._chunk_size = chunk_size

    def __iter__(self) -> Iterator:
        return self

    def __next__(self) -> T:
        return self.next()

    def next(self, timeout=None) -> T:
        """Returns the next chunk from the iterable.
        This method is not thread-safe.
        :raises TimeoutError: if timeout is given and no value is acknowledged in the mean time.
        """
        def peek(iterable):
            "peek at first element of iterable to determine if it is empty"
            try:
                first = next(iterable)
            except StopIteration:
                return None
            return chain([first], iterable)

        if not self._sem.acquire(timeout=timeout):
            raise TimeoutError('Too many values un-acknowledged.')

        next_slice = islice(self._it, self._chunk_size)
        next_slice = peek(next_slice)

        if next_slice:
            return next_slice
        else:
            raise StopIteration
            
    def processed(self):
        """Acknowledges one value allowing another one to be yielded.
        This method is thread-safe.
        """
        self._sem.release()

class Chunks(Iterator):
    """Limits the number of chunks to yield until yielded values are
    acknowledged.
    """

    def __init__(self, it: Iterable, chunk_size: int = 1):
        self._it = iter(it)
        self._chunk_size = chunk_size

    def __iter__(self) -> Iterator:
        return self

    def __next__(self) -> T:
        return self.next()

    def next(self) -> T:
        """Returns the next chunk from the iterable.
        This method is not thread-safe.
        :raises TimeoutError: if timeout is given and no value is acknowledged in the mean time.
        """
        def peek(iterable):
            "peek at first element of iterable to determine if it is empty"
            try:
                first = next(iterable)
            except StopIteration:
                return None
            return chain([first], iterable)

        next_slice = islice(self._it, self._chunk_size)
        next_slice = peek(next_slice)

        if next_slice:
            return next_slice
        else:
            raise StopIteration