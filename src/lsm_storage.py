from src.mem_table import MemTable
from src.record import Record


class LsmStorage:
    def __init__(self):
        self.mem_table = MemTable()
        self.immutable_mem_tables: list[MemTable] = []

    def put(self, key: Record.Key, value: Record.Value):
        self.mem_table.put(key=key, record=Record(key=key, value=value))
        # TODO: check size of memtable here and freeze it if above threshold

    def get(self, key: Record.Key) -> Record.Value:
        return self.mem_table.get(key=key).value
