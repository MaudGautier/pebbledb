from src.red_black_tree import RedBlackTree


def test_insert_root():
    # GIVEN
    tree = RedBlackTree()

    # WHEN
    tree.insert(data=10)

    # THEN
    values = tree.read_data()
    expected_values = [10, None, None]
    assert values == expected_values

