from src.record import Record


class WriteAheadLog:
    def __init__(self, path: str):
        self.path = path
        self.file = open(path, "ab",
                         buffering=0)  # setting the buffer size to 0 so that it flushes right after writing

    def insert(self, record: Record):
        self.file.write(record.to_bytes())
