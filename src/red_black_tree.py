from collections import deque

Value = int


class Node:
    def __init__(self, value: Value, left: "Node" or None = None, right: "Node" or None = None):
        self.value = value
        self.left = left
        self.right = right

    def insert(self, value: Value):
        # TODO: ignoring equal case for now
        if value < self.value:
            left_node = self.left
            if left_node is None:
                self.left = Node(value=value)
                return
            left_node.insert(value)
        elif value > self.value:
            right_node = self.right
            if right_node is None:
                self.right = Node(value=value)
                return
            right_node.insert(value)

        # TODO: some rotations needed as we go


# ---- Test Utils ----
def bfs(root: Node):
    nodes_to_visit = deque()
    nodes_to_visit.append(root)
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
