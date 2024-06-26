"""Gong Authentication."""

import base64
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.authenticators import BearerTokenAuthenticator


class GongAuthenticator(BearerTokenAuthenticator):
    """Authenticator class for Gong."""

    @classmethod
    def create_for_stream(cls, stream) -> "GongAuthenticator":
        auth_token = stream.config['access_token']
        return cls(stream=stream, token=auth_token)

    @property
    def auth_header(self) -> str:
        return f"Bearer {self.token}"

