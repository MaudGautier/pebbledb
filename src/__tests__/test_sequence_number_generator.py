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


def test_sequence_number_generator_is_a_singleton_even_if_called_after_the_other():
    SequenceNumberGenerator.reset()
    # GIVEN
    generator1 = SequenceNumberGenerator()
    assert next(generator1) == 0
    assert next(generator1) == 1
    generator2 = SequenceNumberGenerator()

    # WHEN/THEN
    assert next(generator2) == 2
    assert next(generator1) == 3


def test_sequence_number_generator_can_be_restarted():
    SequenceNumberGenerator.reset()
    # GIVEN
    generator1 = SequenceNumberGenerator()
    assert next(generator1) == 0
    assert next(generator1) == 1
    assert next(generator1) == 2

    # WHEN
    generator2 = SequenceNumberGenerator()
    SequenceNumberGenerator.reset()
    generator3 = SequenceNumberGenerator()

    # THEN
    assert next(generator1) == 3
    assert next(generator2) == 4
    assert next(generator3) == 0
