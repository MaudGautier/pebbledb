from collections import deque

Value = int


class Node:
    def __init__(self, value: Value, left: "Node" or None = None, right: "Node" or None = None):
        self.value = value
        self.left = left
        self.right = right


class RedBlackTree:
    def __init__(self, root: Node or None = None):
        self.root = root

    def insert(self, value: Value):
        node = self.root
        if value < node.value:
            left_node = node.left
            if left_node is None:
                node.left = Node(value=value)
                return
            left_node.insert(value)
        elif value > node.value:
            right_node = node.right
            if right_node is None:
                node.right = Node(value=value)
                return
            right_node.insert(value)

        # TODO: some rotations needed as we go


# ---- Test Utils ----
def bfs(node: Node):
    nodes_to_visit = deque()
    nodes_to_visit.append(node)
    nodes_to_display = []

    while nodes_to_visit:
        current = nodes_to_visit.popleft()

        nodes_to_display.append(current)

        if current is None:
            continue

        left = current.left
        right = current.right
        nodes_to_visit.append(left)
        nodes_to_visit.append(right)

    return nodes_to_display


def extract_values_from_list_of_nodes(nodes: list[Node or None]) -> list[int or None]:
    values = []
    for node in nodes:
        values.append(node.value if node is not None else None)
    return values
