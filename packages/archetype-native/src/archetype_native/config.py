"""Immutable provider data profile. Credentials are captured only at activation."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urlsplit


@dataclass(frozen=True, slots=True)
class RemoteData:
    """Operator-owned remote data placement; local catalog authority is retained."""

    version: int
    uri: str
    region: str
    path_style_access: bool
    credential_source: str = "aws_environment"
    endpoint: str | None = None

    def __post_init__(self):
        if type(self.version) is not int or self.version != 1:
            raise ValueError("Unsupported remote data configuration")
        if type(self.uri) is not str or len(self.uri) > 2048:
            raise ValueError("Invalid remote data URI")
        match = re.fullmatch(r"s3://([a-z0-9.-]{3,63})/([A-Za-z0-9_./-]+)", self.uri)
        if not match or any(part in {"", ".", ".."} for part in match[2].split("/")):
            raise ValueError("Expected canonical S3 bucket and owned prefix")
        if type(self.region) is not str or not re.fullmatch(r"[a-z0-9-]{1,64}", self.region):
            raise ValueError("Invalid provider region")
        if (
            type(self.path_style_access) is not bool
            or type(self.credential_source) is not str
            or self.credential_source != "aws_environment"
        ):
            raise ValueError("Unsupported credential source or addressing mode")
        if self.endpoint is not None:
            if type(self.endpoint) is not str or len(self.endpoint) > 1024:
                raise ValueError("Invalid provider endpoint")
            try:
                parsed = urlsplit(self.endpoint)
                valid = (
                    parsed.scheme == "https"
                    and parsed.hostname
                    and parsed.username is None
                    and parsed.password is None
                    and not parsed.query
                    and not parsed.fragment
                    and parsed.path in {"", "/"}
                    and parsed.port != 0
                    and "?" not in self.endpoint
                    and "#" not in self.endpoint
                    and not any(ord(c) < 33 or ord(c) == 127 or c.isspace() for c in self.endpoint)
                )
            except ValueError:
                valid = False
            if not valid:
                raise ValueError("Expected HTTPS provider origin without credentials")

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> RemoteData:
        required = {"version", "uri", "region", "path_style_access", "credential_source"}
        if (
            type(value) is not dict
            or not required <= value.keys()
            or value.keys() - required - {"endpoint"}
        ):
            raise ValueError("Unexpected remote data configuration fields")
        return cls(**value)
