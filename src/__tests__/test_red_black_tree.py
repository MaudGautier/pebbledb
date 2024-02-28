from src.red_black_tree import Node, extract_values_from_list_of_nodes, bfs, RedBlackTree


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
