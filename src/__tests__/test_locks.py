import threading
import time

import pytest

from src.locks import ReadWriteLock


class ClassToTest:
    def __init__(self):
        self.state = {"value": 0}
        self.state_lock = ReadWriteLock()


def test_concurrent_write_locks_are_serialized():
    shared_object = ClassToTest()
    # This list will store timestamps to log when each write operation starts and ends
    timestamps = []

    def write_task(task_id):
        with shared_object.state_lock.write():
            # Log the start time of the write operation
            start_time = time.time()
            # Simulate some work that takes a bit of time
            time.sleep(0.1)
            # Log the end time of the write operation
            end_time = time.time()
            timestamps.append((task_id, start_time, end_time))

    # Create two threads for write tasks
    writer1 = threading.Thread(target=lambda: write_task(1))
    writer2 = threading.Thread(target=lambda: write_task(2))
    writer3 = threading.Thread(target=lambda: write_task(3))
    writer4 = threading.Thread(target=lambda: write_task(4))

    # Start the threads
    writer1.start()
    writer2.start()
    writer3.start()
    writer4.start()

    # Wait for both threads to complete
    writer1.join()
    writer2.join()
    writer3.join()
    writer4.join()

    # Ensure that the write operations did not overlap
    # Sort the timestamps list by start time to analyze in order
    timestamps.sort(key=lambda x: x[1])
    for i in range(len(timestamps) - 1):
        # Check if the end time of the first operation is before the start time of the next
        # This ensures serialization of write operations
        assert timestamps[i][2] <= timestamps[i + 1][
            1], f"Write operations overlapped: {timestamps[i]} and {timestamps[i + 1]}"


def test_concurrent_read_locks_are_not_serialized():
    shared_object = ClassToTest()
    # This list will store timestamps to log when each write operation starts and ends
    timestamps = []

    def read_task(task_id):
        with shared_object.state_lock.read():
            # Log the start time of the write operation
            start_time = time.time()
            # Simulate some work that takes a bit of time
            time.sleep(0.01)
            # Log the end time of the write operation
            end_time = time.time()
            timestamps.append((task_id, start_time, end_time))

    # Create readers threads
    readers = [threading.Thread(target=lambda i=i: read_task(i)) for i in range(200)]

    # Start the threads
    for reader in readers:
        reader.start()

    # Wait for all threads to complete
    for reader in readers:
        reader.join()

    # Ensure that the read operations overlapped
    # Sort the timestamps list by start time to analyze in order
    timestamps.sort(key=lambda x: x[1])
    for i in range(len(timestamps) - 1):
        # Check if the end time of the first operation is after the start time of the next
        # This ensures NON serialization of read operations
        assert timestamps[i][2] >= timestamps[i + 1][
            1], f"Read operations were serialized: {timestamps[i]} and {timestamps[i + 1]}"


def test_concurrent_read_and_write_locks():
    shared_object = ClassToTest()
    timestamps_readers1 = []
    timestamps_readers2 = []
    timestamp_writer = []

    def read_task(task_id):
        with shared_object.state_lock.read():
            start_time = time.time()
            time.sleep(0.001)
            end_time = time.time()
            if task_id.startswith("r1"):
                timestamps_readers1.append((task_id, start_time, end_time))
            elif task_id.startswith("r2"):
                timestamps_readers2.append((task_id, start_time, end_time))

    def write_task(task_id):
        with shared_object.state_lock.write():
            start_time = time.time()
            time.sleep(0.001)
            end_time = time.time()
            timestamp_writer.append((task_id, start_time, end_time))

    writer = threading.Thread(target=lambda: write_task("WRITER"))
    readers1 = [threading.Thread(target=lambda i=i: read_task("r1_" + str(i))) for i in range(5)]
    readers2 = [threading.Thread(target=lambda i=i: read_task("r2_" + str(i))) for i in range(3)]

    # Start the threads
    for reader in readers1:
        reader.start()
    writer.start()
    for reader in readers2:
        reader.start()

    # Wait for all threads to complete
    writer.join()
    for reader in readers1:
        reader.join()
    for reader in readers2:
        reader.join()

    # Ensure that all readers are done before writer starts
    for i in range(len(timestamps_readers1) - 1):
        assert timestamps_readers1[i][2] <= timestamp_writer[0][1]
    for i in range(len(timestamps_readers2) - 1):
        assert timestamps_readers2[i][2] <= timestamp_writer[0][1]


def test_concurrent_read_and_write_locks_2():
    shared_object = ClassToTest()
    timestamps_readers1 = []
    timestamps_readers2 = []
    timestamp_writer = []

    def read_task(task_id):
        with shared_object.state_lock.read():
            start_time = time.time()
            time.sleep(0.001)
            end_time = time.time()
            if task_id.startswith("r1"):
                timestamps_readers1.append((task_id, start_time, end_time))
            elif task_id.startswith("r2"):
                timestamps_readers2.append((task_id, start_time, end_time))

    def write_task(task_id):
        with shared_object.state_lock.write():
            start_time = time.time()
            time.sleep(0.001)
            end_time = time.time()
            timestamp_writer.append((task_id, start_time, end_time))

    writer = threading.Thread(target=lambda: write_task("WRITER"))
    readers1 = [threading.Thread(target=lambda i=i: read_task("r1_" + str(i))) for i in range(5)]
    readers2 = [threading.Thread(target=lambda i=i: read_task("r2_" + str(i))) for i in range(3)]

    # Start the threads
    writer.start()
    for reader in readers1:
        reader.start()
    for reader in readers2:
        reader.start()

    # Wait for all threads to complete
    writer.join()
    for reader in readers1:
        reader.join()
    for reader in readers2:
        reader.join()

    # Ensure that all readers begin after writer is done
    for i in range(len(timestamps_readers1) - 1):
        assert timestamp_writer[0][2] <= timestamps_readers1[i][1]
    for i in range(len(timestamps_readers2) - 1):
        assert timestamp_writer[0][2] <= timestamps_readers2[i][1]
