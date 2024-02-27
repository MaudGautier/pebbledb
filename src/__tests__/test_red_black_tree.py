from src.red_black_tree import Node, extract_values_from_list_of_nodes, bfs


def test_insert_one_node():
    # GIVEN
    root = Node(value=10)

    # WHEN
    root.insert(Node(11))

    # THEN
    values = extract_values_from_list_of_nodes(bfs(root=root))
    expected_values = [10, None, 11, None, None]
    assert values == expected_values

