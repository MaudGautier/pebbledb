import os

from src.record import Record


class WriteAheadLog:
    def __init__(self, path: str, file):
        self.path = path
        self.file = file

    @classmethod
    def create(cls, path: str) -> "WriteAheadLog":
        if cls._exists(path):
            raise ValueError(f"Cannot create the WAL because there is already one at {path}")

        file = open(path, "ab", buffering=0)  # setting the buffer size to 0 so that it flushes right after writing)

        return cls(path, file=file)

    @classmethod
    def open(cls, path: str) -> "WriteAheadLog":
        if not cls._exists(path):
            raise ValueError(f"Cannot open the WAL because there is none at {path}")

        file = open(path, "rb")

        return cls(path=path, file=file)

    @staticmethod
    def _exists(path: str) -> bool:
        return os.path.isfile(path)

    def insert(self, record: Record):
        self.file.write(record.to_bytes())

# def read(self):

# def delete(self):

# WriteAheadLog.create(path)
# WriteAheadLog.open(path)
