import pytest

from src.red_black_tree import RedBlackTree, Color, Node


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


def test_get_uncle():
    #     4
    #    / \
    #   2   5
    #  / \
    # 1   3

    # GIVEN
    node1 = Node(data=1)
    node2 = Node(data=2)
    node3 = Node(data=3)
    node4 = Node(data=4)
    node5 = Node(data=5)
    node4.left = node2
    node4.right = node5
    node2.left = node1
    node2.right = node3
    node1.parent = node2
    node3.parent = node2
    node2.parent = node4
    node5.parent = node4

    # WHEN/THEN
    assert node1.uncle is node5
    assert node3.uncle is node5
    with pytest.raises(ValueError) as error_node4:
        _ = node4.uncle
    with pytest.raises(ValueError) as error_node5:
        _ = node5.uncle
    with pytest.raises(ValueError) as error_node2:
        _ = node2.uncle
    assert repr(error_node4.value) == """ValueError('Searching for the uncle of the root. This should not happen!')"""
    assert repr(error_node5.value) == """ValueError('Searching for the sibling of the root. This should not happen!')"""
    assert repr(error_node2.value) == """ValueError('Searching for the sibling of the root. This should not happen!')"""

