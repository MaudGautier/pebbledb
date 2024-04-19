import os

import pytest

from src.__fixtures__.constants import TEST_DIRECTORY
from src.sequence_number_generator import SequenceNumberGenerator


def cleanup_files():
    for filename in os.listdir(TEST_DIRECTORY):
        os.remove(f"{TEST_DIRECTORY}/{filename}")


@pytest.fixture(autouse=True)
def clean_files(request):
    yield

    # Cleanup code
    request.addfinalizer(cleanup_files)


@pytest.fixture(autouse=True)
def reset_sequence_number_generator(request):
    SequenceNumberGenerator.reset()

    yield
