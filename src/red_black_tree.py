from collections import deque
from enum import Enum

Value = int


class Color(str, Enum):
    RED = "RED"
    BLACK = "BLACK"


class Node:
    # TODO: default for insertion is red. Not sure that's what we want as default in the init
    def __init__(self, value: Value, left: "Node" or None = None, right: "Node" or None = None,
                 color: Color = Color.RED):
        self.value = value
        self.left = left
        self.right = right
        self.color = color

    def insert(self, value: Value):
        if value < self.value:
            left_node = self.left
            if left_node is None:
                self.left = Node(value=value, color=Color.RED)
                return
            left_node.insert(value)
        elif value > self.value:
            right_node = self.right
            if right_node is None:
                self.right = Node(value=value, color=Color.RED)
                return
            right_node.insert(value)



class RedBlackTree:
    def __init__(self, root: Node or None = None):
        self.root = root

    def insert(self, value: Value):
        node = self.root
        node.insert(value)




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
