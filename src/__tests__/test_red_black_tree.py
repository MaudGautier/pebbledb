from typing import cast
from unittest.mock import MagicMock

import pytest
from unittest import mock

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


def test_insert_node_on_right_right():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=11)

    # WHEN
    tree.insert(data=12)

    # THEN
    values = tree.read_data()
    expected_values = [10, None, 11, None, 12, None, None]
    assert values == expected_values


def test_insert_node_on_right_left():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=12)

    # WHEN
    tree.insert(data=11)

    # THEN
    values = tree.read_data()
    expected_values = [10, None, 12, 11, None, None, None]
    assert values == expected_values


def test_insert_node_on_left_left():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=9)

    # WHEN
    tree.insert(data=8)

    # THEN
    values = tree.read_data()
    expected_values = [10, 9, None, 8, None, None, None]
    assert values == expected_values


def test_insert_node_on_left_right():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=8)

    # WHEN
    tree.insert(data=9)

    # THEN
    values = tree.read_data()
    expected_values = [10, 8, None, None, 9, None, None]
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
    with pytest.raises(ValueError) as error_n4:
        _ = node4.uncle
    with pytest.raises(ValueError) as error_n5:
        _ = node5.uncle
    with pytest.raises(ValueError) as error_n2:
        _ = node2.uncle
    assert repr(error_n4.value) == "ValueError('Searching for the uncle of the root. This should not happen!')"
    assert repr(error_n5.value) == 'ValueError("Searching for the uncle of the root\'s child. This should not happen!")'
    assert repr(error_n2.value) == 'ValueError("Searching for the uncle of the root\'s child. This should not happen!")'


def test_insert_node_right_right_calls_left_rotation_on_grand_parent():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=11)
    grand_parent = tree.root

    # WHEN/THEN
    with mock.patch.object(tree, 'rotate_left', wraps=tree.rotate_left) as mocked_rotate_left:
        tree.insert(data=12)
        mocked_rotate_left.assert_called_once_with(grand_parent)


def test_insert_node_left_left_calls_right_rotation_on_grand_parent():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=9)
    grand_parent = tree.root

    # WHEN/THEN
    with mock.patch.object(tree, 'rotate_right', wraps=tree.rotate_right) as mocked_rotate_right:
        tree.insert(data=8)
        mocked_rotate_right.assert_called_once_with(grand_parent)


def test_insert_node_right_left_calls_right_rotation_on_parent_and_RR_case():
    with mock.patch.multiple(RedBlackTree, rotate_left=MagicMock(), rotate_right=MagicMock()):
        # GIVEN
        tree = RedBlackTree()
        tree.insert(data=10)
        tree.insert(data=12)
        grand_parent = tree.root
        parent = tree.root.right

        # WHEN/THEN
        tree.insert(data=11)
        rotate_right_mock = cast(MagicMock, tree.rotate_right)
        rotate_left_mock = cast(MagicMock, tree.rotate_left)
        rotate_right_mock.assert_called_once_with(parent)
        rotate_left_mock.assert_called_once_with(grand_parent)


def test_insert_node_left_right_calls_left_rotation_on_parent_and_LL_case():
    with mock.patch.multiple(RedBlackTree, rotate_left=MagicMock(), rotate_right=MagicMock()):
        # GIVEN
        tree = RedBlackTree()
        tree.insert(data=10)
        tree.insert(data=8)
        grand_parent = tree.root
        parent = tree.root.left

        # WHEN/THEN
        tree.insert(data=9)
        rotate_right_mock = cast(MagicMock, tree.rotate_right)
        rotate_left_mock = cast(MagicMock, tree.rotate_left)
        rotate_left_mock.assert_called_once_with(parent)
        rotate_right_mock.assert_called_once_with(grand_parent)
