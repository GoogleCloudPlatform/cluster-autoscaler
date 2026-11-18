# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Release versions and the error raised by the release tooling"""

import re
from typing import NamedTuple

TAG_PREFIX = "cluster-autoscaler-v"
# Version components are ASCII digits without leading zeros, one version has a single tag name.
RELEASE_TAG_PATTERN = re.compile(rf"{TAG_PREFIX}(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


class ErrorOccurred(Exception):
    pass


class Version(NamedTuple):
    """The version of a release tag, cluster-autoscaler-v<major>.<minor>.<patch>.

    The major version names the release track, see manifest.py: v37.x.y belongs to track 1.37.
    """

    major: int
    minor: int
    patch: int

    @classmethod
    def match(cls, tag: str) -> "Version | None":
        match = RELEASE_TAG_PATTERN.fullmatch(tag)
        if not match:
            return None
        major, minor, patch = map(int, match.groups())
        return cls(major, minor, patch)

    @classmethod
    def parse(cls, tag: str) -> "Version":
        version = cls.match(tag)
        if version is None:
            raise ErrorOccurred(f"{tag!r} does not match {RELEASE_TAG_PATTERN.pattern}")
        return version

    @property
    def track(self) -> str:
        """cluster-autoscaler-v37.x.y belongs to "1.37"."""
        return f"1.{self.major}"

    @property
    def tag(self) -> str:
        return f"{TAG_PREFIX}{self}"

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"
