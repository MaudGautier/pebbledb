from src.sequence_number_generator import SequenceNumberGenerator


def test_sequence_number_generator_is_a_singleton():
    # GIVEN
    generator1 = SequenceNumberGenerator()
    generator2 = SequenceNumberGenerator()

    # WHEN/THEN
    assert next(generator1) == 0
    assert next(generator2) == 1
    assert next(generator2) == 2
    assert next(generator1) == 3


def test_sequence_number_generator_can_be_restarted():
    # GIVEN
    generator1 = SequenceNumberGenerator()
    assert next(generator1) == 0
    assert next(generator1) == 1
    assert next(generator1) == 2

    # WHEN
    SequenceNumberGenerator.reset()
    generator2 = SequenceNumberGenerator()

    # THEN
    assert next(generator2) == 0
    assert next(generator1) == 3
