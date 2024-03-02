from collections import deque
from enum import Enum
from typing import Optional

Data = int


class Color(str, Enum):
    RED = "RED"
    BLACK = "BLACK"


class Node:
    def __init__(self,
                 data: Data or None,
                 color: Color = Color.RED,
                 left: Optional["Node"] = None,
                 right: Optional["Node"] = None,
                 ):
        self.data = data
        self.left = left
        self.right = right
        self.color = color
        self.parent: Optional["Node"] = None

    @property
    def uncle(self):
        parent = self.parent
        if parent is None:
            raise ValueError("Searching for the uncle of the root. This should not happen!")

        grand_parent = self.grand_parent
        if grand_parent is None:
            raise ValueError("Searching for the uncle of the root's child. This should not happen!")

        if parent is grand_parent.left:
            return grand_parent.right

        assert parent is grand_parent.right
        return grand_parent.left

    @property
    def grand_parent(self):
        parent = self.parent
        if parent is None:
            raise ValueError("Parent does not exist. This should not happen!")

        return parent.parent


class RedBlackTree:
    NIL_LEAF = Node(data=None, color=Color.BLACK)

    def __init__(self):
        self.root = self.NIL_LEAF

    def _bst_insert(self, node: Node) -> None:
        # Find insert position (find node's parent)
        parent = None
        current = self.root
        while current is not self.NIL_LEAF:
            parent = current
            if current.data < node.data:
                current = current.right
            elif current.data > node.data:
                current = current.left
            elif current.data == node.data:
                raise NotImplementedError()

        # Insert node
        node.parent = parent
        if parent is None:
            self.root = node
        elif parent.data < node.data:
            parent.right = node
        else:
            parent.left = node

    def insert(self, data: Data) -> None:
        new_node = Node(data=data, left=self.NIL_LEAF, right=self.NIL_LEAF)
        self._bst_insert(node=new_node)

        # If root: Recolor to black
        if self.root is new_node:
            new_node.color = Color.BLACK
            return

        # If parent is black => nothing to do
        if new_node.parent.color == Color.BLACK:
            return

        # If parent is red => check for uncle's color
        assert new_node.parent.color == Color.RED
        if new_node.uncle.color == Color.BLACK:
            # Four possible cases to handle:
            if new_node is new_node.grand_parent.left.left:
                print("CASE LL")
                self.rotate_right(new_node.grand_parent)
            elif new_node is new_node.grand_parent.left.right:
                print("CASE LR")
                self.rotate_left(new_node.parent)
                self.rotate_right(new_node.grand_parent)
            elif new_node is new_node.grand_parent.right.left:
                print("CASE RL")
                self.rotate_right(new_node.parent)
                self.rotate_left(new_node.grand_parent)
            elif new_node is new_node.grand_parent.right.right:
                print("CASE RR")
                self.rotate_left(new_node.grand_parent)

        else:
            assert new_node.uncle.color == Color.RED

    def rotate_left(self, x: Node):
        pass


    def rotate_right(self, x: Node):
        pass


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
