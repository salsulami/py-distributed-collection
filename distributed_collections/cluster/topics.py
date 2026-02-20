from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any


class ClusterTopicMixin:
    """
    Distributed topic internals.
    """

    def _dispatch_topic(self, topic_name: str, message: Any) -> None:
        """
        Deliver one topic message to all local subscribers.
        """
        with self._topic_lock:
            callbacks = list(self._topic_subscribers.get(topic_name, {}).values())
        for callback in callbacks:
            try:
                callback(message)
            except Exception:
                continue

    def _topic_subscribe(self, topic_name: str, callback: Callable[[Any], None]) -> str:
        """
        Register local topic callback and return subscription ID.
        """
        subscription_id = uuid.uuid4().hex
        with self._topic_lock:
            registry = self._topic_subscribers.setdefault(topic_name, {})
            registry[subscription_id] = callback
        return subscription_id

    def _topic_unsubscribe(self, topic_name: str, subscription_id: str) -> bool:
        """
        Remove topic callback by subscription ID.
        """
        with self._topic_lock:
            registry = self._topic_subscribers.get(topic_name)
            if not registry:
                return False
            return registry.pop(subscription_id, None) is not None

    def _topic_publish(self, topic_name: str, message: Any) -> None:
        """Publish topic message under configured consistency strategy."""
        self._submit_local_operation(
            collection="topic",
            name=topic_name,
            action="publish",
            values={"message": message},
        )
