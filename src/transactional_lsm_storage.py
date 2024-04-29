from typing import Iterator, Optional

from src.iterators import MergingIterator
from src.lsm_storage import LsmStorage, LsmState
from src.manifest import Configuration, Manifest
from src.record import Record


class TransactionalLsmStorage(LsmStorage):
    def __init__(self,
                 last_committed_sequence_number: int,
                 **kwargs,
                 ):
        super().__init__(**kwargs)
        self.last_committed_sequence_number: int = last_committed_sequence_number

    @classmethod
    def create(cls, **kwargs) -> "TransactionalLsmStorage":

        lsm_storage = LsmStorage.create(**kwargs)

        last_committed_sequence_number = -1

        return cls(configuration=lsm_storage.manifest.configuration,
                   directory=lsm_storage.directory,
                   state=lsm_storage.state,
                   manifest=lsm_storage.manifest,
                   last_committed_sequence_number=last_committed_sequence_number)

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

    @classmethod
    def reconstruct(cls, manifest_path: str) -> "TransactionalLsmStorage":
        lsm_storage = LsmStorage.reconstruct_from_manifest(manifest_path=manifest_path)

        last_committed_sequence_number = cls._get_last_committed_sequence_number(state=lsm_storage.state)

        return cls(
            configuration=lsm_storage.manifest.configuration,
            directory=lsm_storage.directory,
            state=lsm_storage.state,
            manifest=lsm_storage.manifest,
            last_committed_sequence_number=last_committed_sequence_number
        )

    @staticmethod
    def _get_last_committed_sequence_number(state: LsmState) -> int:
        max_sequence_number = -1
        for level_ss_tables in [state.sstables_level0, *state.sstables_levels]:
            for sstable in level_ss_tables:
                max_sequence_number = max(max_sequence_number, sstable.max_sequence_number)

        return max_sequence_number
