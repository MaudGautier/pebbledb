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

    def get(self, key: Record.Key):
        raise NotImplementedError()
