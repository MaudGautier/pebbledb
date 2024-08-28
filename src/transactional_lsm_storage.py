from typing import Iterator, Optional

from src.iterators import MergingIterator
from src.lsm_storage import LsmStorage, LsmState
from src.manifest import Configuration, Manifest
from src.record import Record


class TransactionalLsmStorage(LsmStorage):
    def __init__(self,
                 configuration: Configuration,
                 directory: str,
                 state: LsmState,
                 manifest: Manifest
                 ):
        super().__init__(configuration=configuration, directory=directory, state=state, manifest=manifest)
        self.last_committed_sequence_number: int = -1

    def put(self, key: Record.Key, value: Record.Value):
        record = self.state.memtable.put(key=key, value=value)
        self.last_committed_sequence_number = record.sequence_number
        self._try_freeze()

    def get(self, key: Record.Key) -> Optional[Record.Value]:
        snapshot = self.last_committed_sequence_number

        value = self._search_memtables(key=key,
                                       memtables=[self.state.memtable, *self.state.immutable_memtables],
                                       snapshot=snapshot)
        if value is not None:
            return value

        for level_ss_tables in [self.state.sstables_level0, *self.state.sstables_levels]:
            value = self._search_ss_tables(key=key, ss_tables=level_ss_tables, snapshot=snapshot)
            if value is not None:
                return value

        return None

    def scan(self, lower: Record.Key, upper: Record.Key) -> Iterator[Record]:
        snapshot = self.last_committed_sequence_number

        active_memtable_iterator = self.state.memtable.scan(lower=lower, upper=upper, snapshot=snapshot)
        immutable_memtables_iterators = [memtable.scan(lower=lower, upper=upper, snapshot=snapshot) for memtable in
                                         self.state.immutable_memtables]
        sstables_iterators = [sstable.scan(lower=lower, upper=upper, snapshot=snapshot)
                              for sstable in self.state.sstables_level0]

        iterators = [active_memtable_iterator] + immutable_memtables_iterators + sstables_iterators
        iterator = MergingIterator(iterators=iterators)

        yield from iterator
