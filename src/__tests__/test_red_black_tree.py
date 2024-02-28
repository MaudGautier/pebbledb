from src.red_black_tree import Node, extract_values_from_list_of_nodes, bfs, RedBlackTree, Color


def test_insert_one_node_on_the_right():
    # GIVEN
    tree = RedBlackTree(root=Node(value=10))

    # WHEN
    tree.insert(11)

    # THEN
    values = extract_values_from_list_of_nodes(bfs(node=tree.root))
    expected_values = [10, None, 11, None, None]
    assert values == expected_values


def test_insert_one_node_on_the_left():
    # GIVEN
    tree = RedBlackTree(root=Node(value=10))

    # WHEN
    tree.insert(9)

    # THEN
    values = extract_values_from_list_of_nodes(bfs(node=tree.root))
    expected_values = [10, 9, None, None, None]
    assert values == expected_values


def test_insert_root_value():
    # GIVEN
    tree = RedBlackTree(root=Node(value=10))

    # WHEN
    tree.insert(10)

    # THEN
    values = extract_values_from_list_of_nodes(bfs(node=tree.root))
    expected_values = [10, None, None]
    assert values == expected_values


def test_insert_one_grandchild_node_on_the_left():
    # INITIAL
    #      10
    #     /
    #    9

    # RESULT
    #         10
    #        /  \
    #       9   None
    #      / \
    #     8  None
    #    / \
    # None None

    # GIVEN
    tree = RedBlackTree(root=Node(value=10, left=Node(value=9)))

    # WHEN
    tree.insert(8)

    # THEN
    values = extract_values_from_list_of_nodes(bfs(node=tree.root))
    expected_values = [10, 9, None, 8, None, None, None]
    assert values == expected_values


def test_can_insert_on_empty_tree():
    # GIVEN
    tree = RedBlackTree(root=None)

    # WHEN
    tree.insert(10)

    # THEN
    values = extract_values_from_list_of_nodes(bfs(node=tree.root))
    expected_values = [10, None, None]
    assert values == expected_values


# RED BLACK TREE - INSERTIONS
def test_insert_root_is_black():
    # GIVEN
    tree = RedBlackTree(root=None)

    # WHEN
    tree.insert(10)

    # THEN
    assert tree.root.color == Color.BLACK


def test_get_uncle_should_select_the_correct_one():
    # GIVEN
    root = Node(value=10)
    left_node = root.insert(9)
    right_node = root.insert(11)
    left_left_node = left_node.insert(8)

    # WHEN
    uncle = left_left_node.get_uncle()

    # THEN
    assert uncle is right_node
    assert uncle is not left_node

def test_get_uncle_should_return_None_if_no_uncle():
    # GIVEN
    root = Node(value=10)
    left_node = root.insert(9)
    left_left_node = left_node.insert(8)

    # WHEN
    uncle = left_left_node.get_uncle()

    # THEN
    assert uncle is None


