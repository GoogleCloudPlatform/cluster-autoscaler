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
"""releases.json, the release tags exported to OSS grouped by release track:

{
  "schema": 1,
  "releases": {
    "1.37": {
      "tags": {
        "cluster-autoscaler-v37.242.0": {
          "date": "<tag date>",
          "gerrit_commit": "<gerrit sha>",
          "github_commit": "<github sha>",
          "qualified": false,
          "release_notes": [
            "<note>"
          ]
        }
      }
    }
  }
}
"""

import dataclasses
import json
import re
from collections.abc import Mapping
from typing import Any

import versions

PATH = "releases.json"
SCHEMA_VERSION = 1
TRACK_PATTERN = re.compile(r"1\.(0|[1-9][0-9]*)")


def _object(data: object, cls: type[Any], where: str) -> Mapping[str, Any]:
    """Checks that data has every field of the dataclass and nothing else, defaults are never assumed."""
    if not isinstance(data, dict):
        raise versions.ErrorOccurred(f"{where}: expected a JSON object")
    names = [field.name for field in dataclasses.fields(cls)]
    unknown = sorted(set(data) - set(names))
    missing = [name for name in names if name not in data]
    if unknown:
        raise versions.ErrorOccurred(f"{where}: unknown keys {' '.join(unknown)}")
    if missing:
        raise versions.ErrorOccurred(f"{where}: missing keys {' '.join(missing)}")
    return data


def _track_key(track: str) -> tuple[int, ...]:
    return tuple(map(int, track.split(".")))


@dataclasses.dataclass
class TagEntry:
    date: str
    gerrit_commit: str
    github_commit: str
    qualified: bool
    release_notes: list[str]

    @classmethod
    def from_dict(cls, data: object, where: str) -> "TagEntry":
        entry = cls(**_object(data, cls, where))
        if not all(
            isinstance(value, str) and value for value in (entry.date, entry.gerrit_commit, entry.github_commit)
        ):
            raise versions.ErrorOccurred(f"{where}: date, gerrit_commit and github_commit must be non-empty strings")
        if not isinstance(entry.qualified, bool):
            raise versions.ErrorOccurred(f"{where}: qualified must be a boolean")
        if not isinstance(entry.release_notes, list) or not all(isinstance(note, str) for note in entry.release_notes):
            raise versions.ErrorOccurred(f"{where}: release_notes must be a list of strings")
        return entry

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class TrackEntry:
    """The releases of one track, keyed by tag name."""

    tags: dict[str, TagEntry] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: object, where: str) -> "TrackEntry":
        tags = _object(data, cls, where)["tags"]
        if not isinstance(tags, dict):
            raise versions.ErrorOccurred(f"{where}: tags must be a JSON object")
        for name in tags:
            versions.Version.parse(name)
        return cls({name: TagEntry.from_dict(entry, f"{where} {name}") for name, entry in tags.items()})

    def to_dict(self) -> dict[str, Any]:
        return {"tags": {name: self.tags[name].to_dict() for name in sorted(self.tags, key=versions.Version.parse)}}

    def newest(self) -> tuple[versions.Version, TagEntry] | None:
        """The release the next appended tag of the track follows, by version, not by position."""
        if not self.tags:
            return None
        name = max(self.tags, key=versions.Version.parse)
        return versions.Version.parse(name), self.tags[name]


@dataclasses.dataclass
class Manifest:
    schema: int = SCHEMA_VERSION
    releases: dict[str, TrackEntry] = dataclasses.field(default_factory=dict)

    @classmethod
    def loads(cls, text: str, where: str = PATH) -> "Manifest":
        try:
            data = _object(json.loads(text), cls, where)
        except json.JSONDecodeError as e:
            raise versions.ErrorOccurred(f"{where}: {e}") from e
        if data.get("schema") != SCHEMA_VERSION:
            raise versions.ErrorOccurred(
                f"{where}: unsupported schema {data.get('schema')!r}, expected {SCHEMA_VERSION}"
            )
        tracks = data["releases"]
        if not isinstance(tracks, dict):
            raise versions.ErrorOccurred(f"{where}: releases must be a JSON object")
        releases = {}
        for track, entry in tracks.items():
            if not TRACK_PATTERN.fullmatch(track):
                raise versions.ErrorOccurred(f"{where}: track {track!r} does not match {TRACK_PATTERN.pattern}")
            releases[track] = TrackEntry.from_dict(entry, f"{where}: {track}")
            misplaced = [name for name in releases[track].tags if versions.Version.parse(name).track != track]
            if misplaced:
                raise versions.ErrorOccurred(f"{where}: {' '.join(misplaced)} do not belong to track {track}")
        manifest = cls(SCHEMA_VERSION, releases)
        if manifest.dumps() != text:
            raise versions.ErrorOccurred(f"{where}: not in the format the tooling writes, appending would rewrite it")
        return manifest

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "releases": {track: self.releases[track].to_dict() for track in sorted(self.releases, key=_track_key)},
        }

    def dumps(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False) + "\n"

    def track(self, track: str) -> TrackEntry:
        return self.releases.get(track, TrackEntry())

    def tags(self) -> list[tuple[versions.Version, TagEntry]]:
        """Every recorded tag of every track, oldest version first."""
        tags = [
            (versions.Version.parse(name), entry)
            for track in self.releases.values()
            for name, entry in track.tags.items()
        ]
        return sorted(tags, key=lambda tag: tag[0])

    def append(self, version: versions.Version, entry: TagEntry) -> None:
        if entry.qualified:
            raise versions.ErrorOccurred(f"{version.tag}: tags are qualified by hand, not when appended")
        tags = self.releases.setdefault(version.track, TrackEntry()).tags
        if version.tag in tags:
            raise versions.ErrorOccurred(f"{version.tag} is already in {PATH}, recorded tags are never modified")
        tags[version.tag] = entry
