from collections import deque
from enum import Enum

Data = int


class Color(str, Enum):
    RED = "RED"
    BLACK = "BLACK"


class Node:

    def __init__(self,
                 data: Data or None,
                 color: Color = Color.RED,
                 left: "Node" or None = None,
                 right: "Node" or None = None,
                 ):
        self.data = data
        self.left = left
        self.right = right
        self.color = color
        self.parent = None


class RedBlackTree:
    NIL_LEAF = Node(data=None, color=Color.BLACK)

    def __init__(self, root: Node or None = None):
        self.root = root

    def insert(self, data: Data):
        if self.root is None:
            self.root = Node(data=data, left=self.NIL_LEAF, right=self.NIL_LEAF)
        else:
            raise NotImplementedError()

    # ---- UTILS FOR TESTS -----
    def read_data(self):
        nodes = self.bfs()
        return [node.data for node in nodes]

    def bfs(self):
        nodes_to_visit = deque()
        nodes_to_visit.append(self.root)
        nodes_to_display = []

        while nodes_to_visit:
            current = nodes_to_visit.popleft()

            nodes_to_display.append(current)

            if current is self.NIL_LEAF:
                continue

            left = current.left
            right = current.right
            nodes_to_visit.append(left)
            nodes_to_visit.append(right)

        return nodes_to_display
