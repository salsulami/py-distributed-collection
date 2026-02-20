from __future__ import annotations

import ssl
from typing import Any


class ClusterSecurityMixin:
    """
    ACL and TLS context helper methods.
    """

    def _is_acl_allowed(self, *, sender_node_id: str, payload: dict[str, Any]) -> bool:
        """
        Check sender-level ACL authorization for one operation payload.
        """
        if not self.config.acl.enabled:
            return True
        collection = str(payload.get("collection", ""))
        action = str(payload.get("action", ""))
        action_key = f"{collection}.{action}"
        permissions = self.config.acl.sender_permissions.get(sender_node_id)
        if permissions is None:
            permissions = self.config.acl.sender_permissions.get("*")
        if permissions is None:
            return bool(self.config.acl.default_allow)
        return ("*" in permissions) or (action_key in permissions)

    def _build_server_ssl_context(self) -> ssl.SSLContext | None:
        """
        Build server SSL context when TLS is enabled.
        """
        if not self.config.tls.enabled:
            return None
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.load_cert_chain(
            certfile=str(self.config.tls.certfile),
            keyfile=str(self.config.tls.keyfile),
        )
        if self.config.tls.ca_file:
            context.load_verify_locations(cafile=str(self.config.tls.ca_file))
        if self.config.tls.require_client_cert:
            context.verify_mode = ssl.CERT_REQUIRED
        else:
            context.verify_mode = ssl.CERT_NONE
        context.check_hostname = False
        return context

    def _build_client_ssl_context(self) -> ssl.SSLContext | None:
        """
        Build client SSL context when TLS is enabled.
        """
        if not self.config.tls.enabled:
            return None
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        if self.config.tls.ca_file:
            context.load_verify_locations(cafile=str(self.config.tls.ca_file))
        else:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        if self.config.tls.certfile and self.config.tls.keyfile:
            context.load_cert_chain(
                certfile=str(self.config.tls.certfile),
                keyfile=str(self.config.tls.keyfile),
            )
        return context
