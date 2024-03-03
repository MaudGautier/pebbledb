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
    tree._bst_insert(node=Node(data=12, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF))

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
    tree._bst_insert(node=Node(data=11, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF))

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
    tree._bst_insert(node=Node(data=8, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF))

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
    tree._bst_insert(node=Node(data=9, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF))

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
    parent = grand_parent.right

    # WHEN/THEN
    with mock.patch.multiple(RedBlackTree, rotate_left=MagicMock(), swap_colors=MagicMock()):
        tree.insert(data=12)
        cast(MagicMock, tree.rotate_left).assert_called_once_with(grand_parent)
        cast(MagicMock, tree.swap_colors).assert_called_once_with(node1=grand_parent, node2=parent)


def test_insert_node_left_left_calls_right_rotation_on_grand_parent():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=9)
    grand_parent = tree.root
    parent = grand_parent.left

    # WHEN/THEN
    with mock.patch.multiple(RedBlackTree, rotate_right=MagicMock(), swap_colors=MagicMock()):
        tree.insert(data=8)
        cast(MagicMock, tree.rotate_right).assert_called_once_with(grand_parent)
        cast(MagicMock, tree.swap_colors).assert_called_once_with(node1=grand_parent, node2=parent)


def test_insert_node_right_left_calls_right_rotation_on_parent_and_RR_case():
    with mock.patch.multiple(RedBlackTree, rotate_left=MagicMock(), rotate_right=MagicMock(), swap_colors=MagicMock()):
        # GIVEN
        tree = RedBlackTree()
        tree.insert(data=10)
        tree.insert(data=12)
        grand_parent = tree.root
        parent = tree.root.right

        # WHEN/THEN
        tree.insert(data=11)
        new_node = parent.left
        rotate_right_mock = cast(MagicMock, tree.rotate_right)
        rotate_left_mock = cast(MagicMock, tree.rotate_left)
        rotate_right_mock.assert_called_once_with(parent)
        rotate_left_mock.assert_called_once_with(grand_parent)
        cast(MagicMock, tree.swap_colors).assert_called_once_with(node1=grand_parent, node2=new_node)


def test_insert_node_left_right_calls_left_rotation_on_parent_and_LL_case():
    with mock.patch.multiple(RedBlackTree, rotate_left=MagicMock(), rotate_right=MagicMock(), swap_colors=MagicMock()):
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
        new_node = parent.right
        cast(MagicMock, tree.swap_colors).assert_called_once_with(node1=grand_parent, node2=new_node)


def test_rotate_right():
    # -------- ORIGINAL --------
    #     X
    #    / \
    #   Y   11
    #  / \
    # 7   9
    #
    # --------  RESULT  --------
    #   Y
    #  / \
    # 7   X
    #    / \
    #   9   11

    # GIVEN
    tree = RedBlackTree()
    node_X = Node(data=10, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    tree.root = node_X
    node_Y = Node(data=8, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    node_7 = Node(data=7, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    node_9 = Node(data=9, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    node_11 = Node(data=11, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)

    tree._bst_insert(node=node_Y)
    tree._bst_insert(node=node_7)
    tree._bst_insert(node=node_9)
    tree._bst_insert(node=node_11)

    # WHEN
    tree.rotate_right(node_X)

    # THEN
    # - first node
    assert node_Y.left is node_7
    assert node_Y.right is node_X
    assert node_Y.parent is None
    # - second node
    assert node_7.left is RedBlackTree.NIL_LEAF
    assert node_7.right is RedBlackTree.NIL_LEAF
    assert node_7.parent is node_Y
    # - third node
    assert node_X.left is node_9
    assert node_X.right is node_11
    assert node_X.parent is node_Y
    # - fourth node
    assert node_9.left is RedBlackTree.NIL_LEAF
    assert node_9.right is RedBlackTree.NIL_LEAF
    assert node_9.parent is node_X
    # - fifth node
    assert node_11.left is RedBlackTree.NIL_LEAF
    assert node_11.right is RedBlackTree.NIL_LEAF
    assert node_11.parent is node_X
    # root
    assert tree.root is node_Y


def test_rotate_right_triangle():
    # -------- ORIGINAL --------
    #     10
    #      \
    #       12 (X)
    #      /
    #     11
    #
    # --------  RESULT  --------
    #   10
    #    \
    #     11
    #      \
    #       12 (X)

    # GIVEN
    tree = RedBlackTree()
    node_10 = Node(data=10, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    tree.root = node_10
    node_12 = Node(data=12, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    node_11 = Node(data=11, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)

    tree._bst_insert(node=node_12)
    tree._bst_insert(node=node_11)

    # WHEN
    tree.rotate_right(node_12)

    # THEN
    # - first node
    assert node_10.left is RedBlackTree.NIL_LEAF
    assert node_10.right is node_11
    assert node_10.parent is None
    # - second node
    assert node_11.left is RedBlackTree.NIL_LEAF
    assert node_11.right is node_12
    assert node_11.parent is node_10
    # - third node
    assert node_12.left is RedBlackTree.NIL_LEAF
    assert node_12.right is RedBlackTree.NIL_LEAF
    assert node_12.parent is node_11
    # root
    assert tree.root is node_10


def test_rotate_left():
    # -------- ORIGINAL --------
    #   X
    #  / \
    # 7   Y
    #    / \
    #   9   11
    #
    # --------  RESULT  --------
    #     Y
    #    / \
    #   X   11
    #  / \
    # 7   9

    # GIVEN
    tree = RedBlackTree()
    node_Y = Node(data=10, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    node_X = Node(data=8, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    tree.root = node_X
    node_7 = Node(data=7, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    node_9 = Node(data=9, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    node_11 = Node(data=11, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)

    tree._bst_insert(node=node_Y)
    tree._bst_insert(node=node_7)
    tree._bst_insert(node=node_9)
    tree._bst_insert(node=node_11)

    # WHEN
    tree.rotate_left(node_X)

    # THEN
    # - first node
    assert node_X.left is node_7
    assert node_X.right is node_9
    assert node_X.parent is node_Y
    # - second node
    assert node_7.left is RedBlackTree.NIL_LEAF
    assert node_7.right is RedBlackTree.NIL_LEAF
    assert node_7.parent is node_X
    assert node_7.grand_parent is node_Y
    # - third node
    assert node_Y.left is node_X
    assert node_Y.right is node_11
    assert node_Y.parent is None
    # - fourth node
    assert node_9.left is RedBlackTree.NIL_LEAF
    assert node_9.right is RedBlackTree.NIL_LEAF
    assert node_9.parent is node_X
    assert node_9.grand_parent is node_Y
    # - fifth node
    assert node_11.left is RedBlackTree.NIL_LEAF
    assert node_11.right is RedBlackTree.NIL_LEAF
    assert node_11.parent is node_Y
    # root
    assert tree.root is node_Y


def test_rotate_left_triangle():
    # -------- ORIGINAL --------
    #     10
    #    /
    #   8 (X)
    #    \
    #     9 (Y)
    #
    # --------  RESULT  --------
    #      10
    #     /
    #    9 (Y)
    #   /
    #  8 (X)
    #

    # GIVEN
    tree = RedBlackTree()
    node_10 = Node(data=10, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    tree.root = node_10
    node_8 = Node(data=8, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)
    node_9 = Node(data=9, left=RedBlackTree.NIL_LEAF, right=RedBlackTree.NIL_LEAF)

    tree._bst_insert(node=node_8)
    tree._bst_insert(node=node_9)

    # WHEN
    print(tree.read_data())
    tree.rotate_left(node_8)
    print(tree.read_data())

    # THEN
    # - first node
    assert node_10.left is node_9
    assert node_10.right is RedBlackTree.NIL_LEAF
    assert node_10.parent is None
    # - second node
    assert node_9.left is node_8
    assert node_9.right is RedBlackTree.NIL_LEAF
    assert node_9.parent is node_10
    # - third node
    assert node_8.left is RedBlackTree.NIL_LEAF
    assert node_8.right is RedBlackTree.NIL_LEAF
    assert node_8.parent is node_9
    # root
    assert tree.root is node_10


def test_insert_node_right_right_renders_correct_colors():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=11)
    root = tree.root
    left = root.left
    right = root.right
    assert root.data == 10
    assert root.color == Color.BLACK
    assert right.data == 11
    assert right.color == Color.RED
    assert left is RedBlackTree.NIL_LEAF

    # WHEN
    tree.insert(data=12)

    # THEN
    root = tree.root
    left = root.left
    right = root.right
    assert root.data == 11
    assert root.color == Color.BLACK
    assert left.data == 10
    assert left.color == Color.RED
    assert right.data == 12
    assert right.color == Color.RED


def test_insert_node_left_left_renders_correct_colors():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=9)
    root = tree.root
    left = root.left
    right = root.right
    assert root.data == 10
    assert root.color == Color.BLACK
    assert left.data == 9
    assert left.color == Color.RED
    assert right is RedBlackTree.NIL_LEAF

    # WHEN
    tree.insert(data=8)

    # THEN
    root = tree.root
    left = root.left
    right = root.right
    assert root.data == 9
    assert root.color == Color.BLACK
    assert right.data == 10
    assert right.color == Color.RED
    assert left.data == 8
    assert left.color == Color.RED


def test_insert_node_right_left_renders_correct_colors():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=12)
    root = tree.root
    left = root.left
    right = root.right
    assert root.data == 10
    assert root.color == Color.BLACK
    assert right.data == 12
    assert right.color == Color.RED
    assert left is RedBlackTree.NIL_LEAF

    # WHEN
    tree.insert(data=11)

    # THEN
    root = tree.root
    left = root.left
    right = root.right
    assert root.data == 11
    assert root.color == Color.BLACK
    assert left.data == 10
    assert left.color == Color.RED
    assert right.data == 12
    assert right.color == Color.RED


def test_insert_node_left_right_renders_correct_colors():
    # GIVEN
    tree = RedBlackTree()
    tree.insert(data=10)
    tree.insert(data=8)
    root = tree.root
    left = root.left
    right = root.right
    assert root.data == 10
    assert root.color == Color.BLACK
    assert right is RedBlackTree.NIL_LEAF
    assert left.data == 8
    assert left.color == Color.RED

    # WHEN
    tree.insert(data=9)

    # THEN
    root = tree.root
    left = root.left
    right = root.right
    assert root.data == 9
    assert root.color == Color.BLACK
    assert left.data == 8
    assert left.color == Color.RED
    assert right.data == 10
    assert right.color == Color.RED
