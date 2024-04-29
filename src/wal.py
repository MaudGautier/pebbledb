import logging
import os
from typing import BinaryIO

from src.checksums import Checksum
from src.record import Record


class WriteAheadLog:
    def __init__(self, path: str, file: BinaryIO):
        self.path = path
        self.file = file

    @classmethod
    def create(cls, path: str) -> "WriteAheadLog":
        if cls._exists(path):
            raise ValueError(f"Cannot create the WAL because there is already one at {path}")

        file = open(path, "ab", buffering=0)  # setting the buffer size to 0 so that it flushes right after writing

        return cls(path, file=file)

    @classmethod
    def open(cls, path: str) -> "WriteAheadLog":
        if not cls._exists(path):
            raise ValueError(f"Cannot open the WAL because there is none at {path}")

        file = open(path, "rb")

        return cls(path=path, file=file)

    def read_records(self) -> list[Record]:
        data = self.file.read()
        records = []
        while len(data):
            checksum_size = Checksum.nb_bytes
            expected_checksum = Checksum.from_bytes(data=data[:checksum_size])
            record, checkpoint = Record.decode_single_record(data[checksum_size:])

            # Check that record is not corrupted - ignore the rest of the WAL if it is corrupted
            record_checksum = Checksum(data=record.to_bytes())
            if record_checksum != expected_checksum:
                logging.info("Record corrupted. Ignoring the rest of the WAL")
                break

            records.append(record)
            data = data[checksum_size + checkpoint:]
        return records

    @staticmethod
    def _exists(path: str) -> bool:
        return os.path.isfile(path)

    def insert(self, record: Record):
        encoded_record = record.to_bytes()
        encoded_checksum = Checksum(data=encoded_record).to_bytes()
        self.file.write(encoded_checksum)
        self.file.write(encoded_record)

    def remove_self(self) -> None:
        self.file.close()
        os.remove(self.path)
