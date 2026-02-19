"""
Custom exceptions used by the distributed collections runtime.

Keeping library-specific errors in one module gives users a predictable
import surface for catching and handling operational edge cases.
"""


class DistributedCollectionsError(Exception):
    """Base error type for all library-level exceptions."""


class ClusterNotRunningError(DistributedCollectionsError):
    """
    Raised when a network operation is requested before node startup.

    Examples include attempting to join, publish to a topic, or replicate a
    mutation when the TCP server has not been started yet.
    """


class ProtocolDecodeError(DistributedCollectionsError):
    """
    Raised when an incoming message is malformed or cannot be decoded.

    This typically indicates wire corruption, cross-version incompatibility,
    or non-cluster traffic reaching the TCP listener.
    """


class ProtocolVersionError(DistributedCollectionsError):
    """
    Raised when an incoming message uses an incompatible protocol version.

    Nodes in one cluster should run protocol-compatible library versions to
    avoid undefined replication or handshake behavior.
    """


class AuthenticationError(DistributedCollectionsError):
    """
    Raised when message authentication fails.

    Authentication can fail because the signature is missing, malformed, or
    computed using a different shared token.
    """


class UnsupportedOperationError(DistributedCollectionsError):
    """
    Raised when a mutation action is unknown for the target collection type.

    The check protects the data store against applying unsupported state
    transitions that could otherwise leave nodes inconsistent.
    """
