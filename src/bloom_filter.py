import struct
from math import log, ceil
from typing import Optional

import mmh3


class BloomFilter:
    """This class implements a bloom filter.
    A bloom filter is a space-efficient probabilistic data structure that is used to hint on the possible presence
    (or guaranteed absence) of an item in a collection.

    A bloom filter is a sequence of bits that are set or unset depending on the items present in the collection.
    Every time we add a key to the collection, we hash it with a given hash function and then take the modulo of the
    resulting hash by the size of the sequence of bits (so that hashed values are distributed over this range).
    The bit corresponding to the hashed key is set.
    The process is repeated for `n` hash functions. Thus, about `n` bits are set for every key included in the
    collection (or possibly fewer if two hash functions turn the same bit on).

    To know if a key is in the collection, we need to hash the key by all hash functions and check the status of all
    those bits in the sequence:
    - If they are all set, then the key may be in the collection;
    - If at least one of them is not set, then we are guaranteed that the key is not in the collection.
    """

    def __init__(self, nb_bytes: int, nb_hash_functions: int, bits: Optional[int] = None):
        # `bits` is bigger than a 4-byte int (it will be `nb_bytes` long), but Python is able to handle this
        self.nb_bytes = nb_bytes
        self.bits_size = 8 * nb_bytes
        self.nb_hash_functions = nb_hash_functions
        self.bits = bits if bits else 0

    def __eq__(self, other):
        if not isinstance(other, BloomFilter):
            return NotImplemented
        return self.bits == other.bits and self.nb_hash_functions == other.nb_hash_functions

    def _hash(self, key: str) -> list[int]:
        """Hashes the key with all hash functions and defines the list of bits that should be set.
        After hashing, we take the modulo of the result by the size of the sequence of bits so that all hash functions
        are mapped to the same output range.
        """
        encoded_key = key.encode(encoding="utf-8")
        selected_bits = []
        for i in range(self.nb_hash_functions):
            hashed_key = mmh3.hash(encoded_key, i)
            selected_bit = hashed_key % self.bits_size
            selected_bits.append(selected_bit)
        return selected_bits

    def _set_bit(self, bit_index: int) -> None:
        assert bit_index <= self.bits_size, "Selected bit is bigger than the size of the bloom filter."
        bit = (1 << bit_index)
        self.bits |= bit

    def _is_bit_set(self, bit_index: int) -> bool:
        bit = (1 << bit_index)
        return (self.bits & bit) == bit

    def add(self, key: str) -> None:
        """Adds a key to the bloom filter.
        """
        bits_to_set = self._hash(key=key)
        for bit in bits_to_set:
            self._set_bit(bit_index=bit)

    def may_contain(self, key: str) -> bool:
        """Returns True if the key may be in the bloom filter, False if it is guaranteed not to be in it.
        """
        bits_to_check = self._hash(key=key)
        for bit in bits_to_check:
            if not self._is_bit_set(bit_index=bit):
                return False
        return True

    def to_bytes(self) -> bytes:
        # 0xFF is 255 (mask to get the lowest byte) - this encodes with little-endianness
        bytes_to_pack = tuple((self.bits >> (8 * i)) & 0xFF for i in range(self.nb_bytes))
        encoded_bits = struct.pack("B" * self.nb_bytes, *bytes_to_pack)
        encoded_nb_hash_functions = struct.pack("B", self.nb_hash_functions)
        return encoded_bits + encoded_nb_hash_functions

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        nb_bytes = len(data) - 1
        nb_hash_functions = struct.unpack("B", data[nb_bytes:])[0]
        unpacked_bytes = struct.unpack("B" * nb_bytes, data[:nb_bytes])
        bits = sum(byte << (8 * i) for i, byte in enumerate(unpacked_bytes))

        return cls(nb_bytes=nb_bytes, nb_hash_functions=nb_hash_functions, bits=bits)

    @classmethod
    def build_from_keys_and_fp_rate(cls, keys: list[str], fp_rate: float) -> "BloomFilter":
        """
        This method returns a bloom filter with optimal parameters given the number of items to add and the desired
        false positive rate.

        Given:
        - n: number of keys in the bloom filter
        - p: false probability rate

        The optimal number of bits can be computed as:
        m = (-n * log(p)) / (log(2)^2)

        The optimal number of hash functions can be computed as:
        k = (m / n) * log(2)

        (Proof for the formulas can be found on wikipedia: https://en.wikipedia.org/wiki/Bloom_filter).
        """
        n = len(keys)
        m = (-n * log(fp_rate)) / (log(2) ** 2)
        k = (m / n) * log(2)

        bloom_filter = cls(nb_bytes=ceil(m / 8), nb_hash_functions=round(k))

        for key in keys:
            bloom_filter.add(key)

        return bloom_filter
