from src.locks import ReadWriteLock, Mutex
from src.mem_table import MemTable
from src.record import Record


class LsmStorage:
    def __init__(self, max_sstable_size: int = 1000):
        self.mem_table = MemTable()
        self.immutable_mem_tables: list[MemTable] = []
        self._state_lock = ReadWriteLock()
        self._freeze_lock = Mutex()
        self._max_sstable_size = max_sstable_size

    def _try_freeze(self):
        """Checks if the mem_table should be frozen or not.
        The mem_table should be frozen if it is bigger than the `self._max_sstable_size` threshold.

        Further explanations on the details of this method:
        - It acquires the `self._freeze_lock` to ensure that only one freeze operation occurs at any given time
          (necessary because concurrency is allowed => two concurrent insert operations could end up doing a freeze)
        - The size of the mem_table is checked twice: once before acquiring the `self._freeze_lock` and a second time
          after acquiring it. This is to avoid taking a lock unnecessarily the first time and thus to avoid impacting
          performance (preventing other operations needing this lock when it is not necessary). Once the condition is
          true and the `self._freeze_lock` is acquired, it is important to check the condition a second time because the
          mem_table might have been frozen by another operation while this one was checking the condition and acquiring
          the lock.
        - It acquires a read lock on the state before reading the mem_table's size. This is in order to comply to the
          concurrency protocol here: the mutex `self._freeze_lock` is for freeze operations, the ReadWriteLock
          `self.state_lock` is for reading/writing on the state.

        Note: another approach would have been to do the following:
        ```
        if approximate_size >= self._max_sstable_size:
            with self._freeze_lock:
                state_read_lock = self._state_lock.read()
                state_read_lock.__enter__()
                latest_approximate_size = self.mem_table.approximate_size
                if latest_approximate_size >= self._max_sstable_size:
                    state_read_lock.__exit__()
                    self._force_freeze_memtable()
                else:
                    state_read_lock.__exit__()
        ```
        In that second approach, the read/write lock is explicitly managed and its release is made _after_ having
        checked the condition (vs in the current implementation, it is released before making the check).
        The trade-off between the two is about concurrency and atomicity:
        - Current implementation: better concurrency, decreased atomicity (a write operation could add/delete records
          and thus change the size of the mem_table while the condition is being checked)
        - Alternative implementation: better atomicity, decreased concurrency (the lock is held longer => that may
          prevent other operations to write).

        Given that this is only an approximate size, and it does not matter much if it is slightly bigger or smaller, I
        opted for better concurrency here (it is no big deal if the size is a bit bigger or smaller when freezing than
        when checked).
        """
        with self._state_lock.read():
            approximate_size = self.mem_table.approximate_size

        if approximate_size >= self._max_sstable_size:
            with self._freeze_lock:
                with self._state_lock.read():
                    latest_approximate_size = self.mem_table.approximate_size
                if latest_approximate_size >= self._max_sstable_size:
                    self._force_freeze_memtable()

    def _force_freeze_memtable(self):
        pass

    def put(self, key: Record.Key, value: Record.Value):
        self.mem_table.put(record=Record(key=key, value=value))
        self._try_freeze()

    def get(self, key: Record.Key) -> Record.Value:
        return self.mem_table.get(key=key).value
