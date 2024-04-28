import pytest

from src.checksums import Checksum


def test_encode_decode():
    # GIVEN
    checksum = Checksum(data=b'Hello, world!')
    encoded_checksum = checksum.to_bytes()

    # WHEN
    decoded_checksum = Checksum.from_bytes(data=encoded_checksum)

    # THEN
    assert decoded_checksum == checksum


def test_raises_error_if_both_data_and_checksum_provided_to_constructor():
    # GIVEN/WHEN/THEN
    with pytest.raises(ValueError) as error:
        Checksum(data=b'data', checksum=1)

    expected_error = """ValueError("Both checksum (1) and data (b'data') were provided, there should be only one!")"""
    assert repr(error.value) == expected_error


def test_raises_error_if_neither_data_nor_checksum_provided_to_constructor():
    # GIVEN/WHEN/THEN
    with pytest.raises(ValueError) as error:
        Checksum()

    expected_error = """ValueError('Neither checksum nor data was provided, one should be provided!')"""
    assert repr(error.value) == expected_error
