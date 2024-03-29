import pytest

from src.sstable import SSTableBuilder


@pytest.fixture
def sstable_four_blocks():
    sstable_builder = SSTableBuilder(sstable_size=20000, block_size=150)
    # Goes into Data Block 0
    sstable_builder.add(key="aaa", value=b'some_long_value_for_aaa')
    sstable_builder.add(key="bbb", value=b'some_long_value_for_bbb')
    sstable_builder.add(key="ccc", value=b'some_long_value_for_ccc')
    sstable_builder.add(key="ddd", value=b'some_long_value_for_ddd')
    # Goes into Data Block 1
    sstable_builder.add(key="eee", value=b'some_long_value_for_eee')
    sstable_builder.add(key="fff", value=b'some_long_value_for_fff')
    sstable_builder.add(key="ggg", value=b'some_long_value_for_ggg')
    sstable_builder.add(key="hhh", value=b'some_long_value_for_hhh')
    # Goes into Data Block 2
    sstable_builder.add(key="iii", value=b'some_long_value_for_iii')
    sstable_builder.add(key="jjj", value=b'some_long_value_for_jjj')
    sstable_builder.add(key="kkk", value=b'some_long_value_for_kkk')
    sstable_builder.add(key="lll", value=b'some_long_value_for_lll')
    # Goes into Data Block 3
    sstable_builder.add(key="mmm", value=b'some_long_value_for_mmm')
    sstable_builder.add(key="nnn", value=b'some_long_value_for_nnn')
    sstable_builder.add(key="ooo", value=b'some_long_value_for_ooo')
    sstable_builder.add(key="ppp", value=b'some_long_value_for_ppp')

    sstable = sstable_builder.build()

    assert len(sstable.meta_blocks) == 4
    assert sstable.meta_blocks[0].first_key == "aaa"
    assert sstable.meta_blocks[0].last_key == "ddd"
    assert sstable.meta_blocks[1].first_key == "eee"
    assert sstable.meta_blocks[1].last_key == "hhh"
    assert sstable.meta_blocks[2].first_key == "iii"
    assert sstable.meta_blocks[2].last_key == "lll"
    assert sstable.meta_blocks[3].first_key == "mmm"
    assert sstable.meta_blocks[3].last_key == "ppp"

    return sstable
