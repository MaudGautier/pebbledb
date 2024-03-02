from src.red_black_tree import RedBlackTree, Color


def test_insert_root():
    # GIVEN
    tree = RedBlackTree()

    # WHEN
    tree.insert(data=10)

    # THEN
    values = tree.read_data()
    expected_values = [10, None, None]
    assert values == expected_values


def test_insert_node_on_left():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)

    # WHEN
    tree.insert(data=9)

    # THEN
    values = tree.read_data()
    expected_values = [10, 9, None, None, None]
    assert values == expected_values


def test_insert_node_on_right():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)

    # WHEN
    tree.insert(data=11)

    # THEN
    values = tree.read_data()
    expected_values = [10, None, 11, None, None]
    assert values == expected_values


def test_inserted_root_is_black():
    # GIVEN
    tree = RedBlackTree()

    # WHEN
    tree.insert(data=10)

    # THEN
    assert tree.root.color == Color.BLACK


def test_node_is_red_when_inserted():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)

    # WHEN
    tree.insert(data=11)
    tree.insert(data=9)

    # THEN
    assert tree.root.color == Color.BLACK
    assert tree.root.left.color == Color.RED
    assert tree.root.right.color == Color.RED
