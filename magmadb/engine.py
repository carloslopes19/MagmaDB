from __future__ import annotations

import threading
from typing import Optional, Dict, Iterator, Tuple


class _Node:
    __slots__ = ("key", "value", "prev", "next")

    def __init__(self, key: bytes, value: bytes) -> None:
        self.key: bytes = key
        self.value: bytes = value
        self.prev: Optional[_Node] = None
        self.next: Optional[_Node] = None


class VoltEngine:
    """LRU-cache engine with O(1) get/set using doubly linked list + hash map."""

    def __init__(self, max_keys: int = 1000) -> None:
        self._max_keys: int = max_keys
        self._cache: Dict[bytes, _Node] = {}
        self._head: Optional[_Node] = None
        self._tail: Optional[_Node] = None
        self._lock = threading.Lock()

    def get(self, key: bytes) -> Optional[bytes]:
        with self._lock:
            node = self._cache.get(key)
            if node is None:
                return None
            self._move_to_head(node)
            return node.value

    def set(self, key: bytes, value: bytes) -> None:
        with self._lock:
            node = self._cache.get(key)
            if node is not None:
                node.value = value
                self._move_to_head(node)
                return
            node = _Node(key=key, value=value)
            self._cache[key] = node
            self._add_to_head(node)
            if len(self._cache) > self._max_keys:
                self._evict_tail()

    def delete(self, key: bytes) -> bool:
        with self._lock:
            node = self._cache.pop(key, None)
            if node is None:
                return False
            self._remove_node(node)
            return True

    def has(self, key: bytes) -> bool:
        with self._lock:
            return key in self._cache

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._head = None
            self._tail = None

    @property
    def size(self) -> int:
        return len(self._cache)

    @property
    def max_keys(self) -> int:
        return self._max_keys

    def snapshot(self) -> Dict[bytes, bytes]:
        """Return a shallow copy of all key-value pairs (thread-safe)."""
        with self._lock:
            return {k: node.value for k, node in self._cache.items()}

    def restore(self, data: Dict[bytes, bytes]) -> None:
        """Replace all state with the given key-value dict."""
        with self._lock:
            self._cache.clear()
            self._head = None
            self._tail = None
            for key, value in data.items():
                node = _Node(key=key, value=value)
                self._cache[key] = node
                self._add_to_head(node)

    def iter_items(self) -> Iterator[Tuple[bytes, bytes]]:
        """Iterate over items from head (most recent) to tail (oldest)."""
        with self._lock:
            current = self._head
            while current is not None:
                yield (current.key, current.value)
                current = current.next

    def _move_to_head(self, node: _Node) -> None:
        if node is self._head:
            return
        self._remove_node(node)
        self._add_to_head(node)

    def _add_to_head(self, node: _Node) -> None:
        node.prev = None
        node.next = self._head
        if self._head is not None:
            self._head.prev = node
        self._head = node
        if self._tail is None:
            self._tail = node

    def _remove_node(self, node: _Node) -> None:
        prev_node = node.prev
        next_node = node.next
        if prev_node is not None:
            prev_node.next = next_node
        else:
            self._head = next_node
        if next_node is not None:
            next_node.prev = prev_node
        else:
            self._tail = prev_node
        node.prev = None
        node.next = None

    def _evict_tail(self) -> None:
        if self._tail is None:
            return
        node = self._tail
        self._cache.pop(node.key, None)
        self._remove_node(node)
