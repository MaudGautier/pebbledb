from collections import deque
from enum import Enum
from typing import Optional, Iterator

from src.locks import Mutex


class Color(str, Enum):
    RED = "RED"
    BLACK = "BLACK"


class Node:
    Key = int or str
    Data = bytes

    def __init__(self,
                 key: Key or None,
                 data: Optional[Data] = None,
                 color: Color = Color.RED,
                 left: Optional["Node"] = None,
                 right: Optional["Node"] = None,
                 ):
        self.key = key
        self.data = data
        self.left = left
        self.right = right
        self.color = color
        self.parent: Optional["Node"] = None
        self.lock = Mutex()

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

    def in_order_traversal(self, lower: Optional[Key] = None, upper: Optional[Key] = None) -> Iterator["Node"]:
        if self is RedBlackTree.NIL_LEAF:
            return

        # Optimization: stop traversing as soon as we reach a node bigger than the upper bound
        if upper is not None and self.key > upper:
            return
        # Optimization: stop traversing as soon as we reach a node smaller than the lower bound
        if lower is not None and self.key < lower:
            return

        yield from self.left.in_order_traversal(lower=lower, upper=upper)
        yield self
        yield from self.right.in_order_traversal(lower=lower, upper=upper)


class RedBlackTree:
    NIL_LEAF = Node(key=None, color=Color.BLACK)

    def __init__(self):
        self.root = self.NIL_LEAF

    def __eq__(self, other) -> bool:
        if not isinstance(other, RedBlackTree):
            return NotImplemented
        return self.read_data(with_value=True) == other.read_data(with_value=True)

    def _get_lower_bound_node(self, key: Node.Key) -> Node:
        node = self.root
        # Find start node
        candidate = self.NIL_LEAF
        while node is not self.NIL_LEAF:
            if node.key == key:
                return node
            elif node.key < key:
                node = node.right
            elif node.key > key:
                if candidate is self.NIL_LEAF or candidate.key > node.key:
                    candidate = node
                node = node.left

        return candidate

    def scan(self, lower: Node.Key, upper: Node.Key) -> Iterator[Node.Data]:
        for node in self.root.in_order_traversal(lower=lower, upper=upper):
            yield node.data

    def __iter__(self) -> Iterator[Node.Data]:
        for node in self.root.in_order_traversal():
            yield node.data

    def _bst_insert(self, node: Node) -> Node:
        # Find insert position (find node's parent)
        parent = None
        current = self.root
        while current is not self.NIL_LEAF:
            with current.lock:
                parent = current
                if current.key < node.key:
                    current = current.right
                elif current.key > node.key:
                    current = current.left
                elif current.key == node.key:
                    current.data = node.data
                    return current

        # Insert node
        node.parent = parent
        if parent is None:
            self.root = node
        elif parent.key < node.key:
            with parent.lock:
                parent.right = node
        else:
            with parent.lock:
                parent.left = node

        return node

    def insert(self, key: Node.Key, data: Optional[Node.Data] = None) -> None:
        new_node = Node(key=key, data=data, left=self.NIL_LEAF, right=self.NIL_LEAF)
        inserted_node = self._bst_insert(node=new_node)
        self._fix_insert(new_node=inserted_node)

    def _fix_insert(self, new_node: Node) -> None:
        """Check if should rebalance, if so: do it"""

        # If root: Recolor to black
        if self.root is new_node:
            with self.root.lock:
                new_node.color = Color.BLACK
            return

        # If parent is black => nothing to do
        if new_node.parent.color == Color.BLACK:
            return

        # If parent is red => check for uncle's color
        grand_parent = new_node.grand_parent
        parent = new_node.parent
        assert new_node.parent.color == Color.RED
        if new_node.uncle.color == Color.BLACK:
            # Four possible cases to handle:
            if new_node is grand_parent.left.left:
                # CASE LL
                self.rotate_right(grand_parent)
                self.swap_colors(node1=grand_parent, node2=parent)
            elif new_node is grand_parent.left.right:
                # CASE LR
                self.rotate_left(parent)
                self.rotate_right(grand_parent)
                self.swap_colors(node1=grand_parent, node2=new_node)
            elif new_node is grand_parent.right.left:
                # CASE RL
                self.rotate_right(parent)
                self.rotate_left(grand_parent)
                self.swap_colors(node1=grand_parent, node2=new_node)
            elif new_node is grand_parent.right.right:
                # CASE RR
                self.rotate_left(grand_parent)
                self.swap_colors(node1=grand_parent, node2=parent)

        else:
            assert new_node.uncle.color == Color.RED
            self._recolor(grand_parent)
            self._fix_insert(grand_parent)

    def rotate_left(self, x: Node) -> None:
        #       Y                                   X
        #      / \          Left-rotate 'X'        / \
        #     a   X       =================>      Y   c
        #        / \                             / \
        #       b   c                           a   b

        y = x.right

        # Rotate on x
        parent = x.parent
        child_to_move = y.left
        with x.lock, y.lock, child_to_move.lock:
            x.right = child_to_move
            y.parent = parent
            x.parent = y
            y.left = x
            child_to_move.parent = x
            if parent is not None:
                with parent.lock:
                    if parent.right is x:
                        parent.right = y
                    elif parent.left is x:
                        parent.left = y

            # Replace root
            if x is self.root:
                self.root = y

    def rotate_right(self, x: Node) -> None:
        #         Y                                   X
        #        / \        Right-rotate 'X'         / \
        #       X   c      =================>       a   Y
        #      / \                                     / \
        #     a   b                                   b   c

        y = x.left

        # Rotate on x
        parent = x.parent
        child_to_move = y.right
        with x.lock, y.lock, child_to_move.lock:
            x.left = child_to_move
            y.parent = parent
            x.parent = y
            y.right = x
            child_to_move.parent = x
            if parent is not None:
                with parent.lock:
                    if parent.right is x:
                        parent.right = y
                    elif parent.left is x:
                        parent.left = y

            # Replace root
            if x is self.root:
                self.root = y

    @staticmethod
    def swap_colors(node1: Node, node2: Node) -> None:
        with node1.lock and node2.lock:
            color1 = node1.color
            node1.color = node2.color
            node2.color = color1

    def _recolor(self, grandparent: Node) -> None:
        with grandparent.lock:
            grandparent.right.color = Color.BLACK
            grandparent.left.color = Color.BLACK
            if grandparent is not self.root:
                grandparent.color = Color.RED

    # ---- UTILS FOR TESTS -----
    def read_data(self, with_value: bool = False) -> list[Node.Key]:
        nodes = self.bfs()
        if with_value:
            return [(node.key, node.data) for node in nodes]
        return [node.key for node in nodes]

    def bfs(self) -> list[Node]:
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

    def get(self, key: Node.Key) -> Optional[Node.Data]:
        node = self.root

        while node is not self.NIL_LEAF:
            if node.key == key:
                return node.data
            elif node.key < key:
                node = node.right
            elif node.key > key:
                node = node.left

        return None
