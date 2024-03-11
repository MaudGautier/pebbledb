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
        self.lock = threading.Lock()
        self.readers = 0
        self.writers = 0
        self.write_requests = 0
        self.condition = threading.Condition(self.lock)

    @contextmanager
    def read(self):
        with self.condition:
            while self.writers > 0 or self.write_requests > 0:
                self.condition.wait()
            self.readers += 1
        yield
        with self.condition:
            self.readers -= 1
            if self.readers == 0:
                self.condition.notify_all()

    @contextmanager
    def write(self):
        with self.condition:
            self.write_requests += 1
            while self.readers > 0 or self.writers > 0:
                self.condition.wait()
            self.write_requests -= 1
            self.writers += 1
        yield
        with self.condition:
            self.writers -= 1
            self.condition.notify_all()


class Mutex:
    def __init__(self):
        self.lock = threading.Lock()

    def __enter__(self):
        self.lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()
