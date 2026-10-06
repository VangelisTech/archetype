"""Provider profile admission is inert, closed and credential free."""

import dataclasses
import unittest
from unittest.mock import patch

from archetype_native import Host
from archetype_native.config import RemoteData


class RemoteConfigTests(unittest.TestCase):
    def profile(self, **changes):
        values = dict(
            version=1, uri="s3://synthetic-bucket/task/case", region="auto", path_style_access=True
        )
        return RemoteData(**(values | changes))

    def test_invalid_namespace_endpoint_and_credential_route_fail_inertly(self):
        for changes in [
            dict(uri=value)
            for value in (
                "s3://synthetic-bucket",
                "s3://synthetic-bucket/",
                "s3://synthetic-bucket/task/../case",
                "s3://synthetic-bucket/task//case",
                "s3://synthetic-bucket/task/case/",
                "s3://synthetic-bucket/task/%2e%2e",
                "s3://key:secret@synthetic-bucket/task",
                "s3://synthetic-bucket/task?secret=yes",
                "file:///tmp/case",
            )
        ] + [
            dict(endpoint="https://key:secret@example.test"),
            dict(endpoint="https://example.test/path"),
            dict(endpoint="http://example.test"),
            dict(endpoint="https://example.test?"),
            dict(endpoint="https://example.test#"),
            dict(endpoint="\x00https://example.test"),
            dict(endpoint="https://example.test:0"),
            dict(credential_source="profile"),
            dict(version=True),
            dict(path_style_access=1),
        ]:
            with (
                self.subTest(changes=changes),
                patch("archetype_native.ctypes.CDLL", side_effect=AssertionError("activation")),
            ):
                with self.assertRaises(ValueError):
                    self.profile(**changes)

    def test_closed_profile_cannot_carry_secret_fields(self):
        profile = self.profile()
        self.assertEqual(RemoteData.from_dict(profile.as_dict()), profile)
        with self.assertRaises(ValueError):
            RemoteData.from_dict(profile.as_dict() | {"secret_access_key": "synthetic"})
        with self.assertRaises(dataclasses.FrozenInstanceError):
            profile.uri = "changed"
        with patch("archetype_native.ctypes.CDLL", side_effect=AssertionError("activation")):
            with self.assertRaises(ValueError):
                Host(
                    library="/tmp/library",
                    store_root="/tmp/store",
                    registry_root=None,
                    build_root=None,
                    driver=None,
                    remote_data=profile.as_dict(),
                )
