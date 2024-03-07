import threading
from contextlib import contextmanager


class ReadWriteLock:
    """A simplified Read-Write Lock:
    - There can be multiple readers at the same time
    - There can be at most one writer
    - A writer prevents readers, and readers prevent writer.

    Thus, at any given point in time, we can have either:
    - one writer but no reader
    - one or multiple readers but no writer
    """

    def __init__(self):
        self.read_lock = threading.Lock()
        self.write_lock = threading.Lock()
        self.readers = 0

    @contextmanager
    def read(self):
        with self.read_lock:
            self.readers += 1
            if self.readers == 1:
                self.write_lock.acquire()
        yield
        with self.read_lock:
            self.readers -= 1
            if self.readers == 0:
                self.write_lock.release()

    @contextmanager
    def write(self):
        self.write_lock.acquire()
        yield
        self.write_lock.release()
