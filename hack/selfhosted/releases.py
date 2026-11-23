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
"""The plan of publish-releases.py: the delta between GitHub and releases.json."""

import dataclasses
import enum
import json
import pathlib
import subprocess
from typing import Any

import manifest
import versions

REPO = "GoogleCloudPlatform/cluster-autoscaler"


class ReleaseState(enum.Enum):
    DRAFT = "draft"
    REGULAR = "regular"
    LATEST = "latest"


@dataclasses.dataclass(frozen=True)
class Release:
    """A release of the manifest, the state it should have on GitHub."""

    version: versions.Version
    state: ReleaseState
    github_state: ReleaseState | None
    date: str
    github_commit: str
    release_notes: tuple[str, ...]
    # The previous release of the track, GitHub generates the notes since that one.
    previous: versions.Version | None

    @property
    def tag(self) -> str:
        return self.version.tag


def gh(*args: str) -> str:
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=True).stdout


def gh_api(endpoint: str) -> Any:
    return json.loads(gh("api", endpoint))


def gh_api_list(endpoint: str) -> list[Any]:
    """'gh api --paginate' prints the pages back to back."""
    output = gh("api", "--paginate", endpoint).strip()
    items, decoder, position = [], json.JSONDecoder(), 0
    while position < len(output):
        page, position = decoder.raw_decode(output, position)
        items.extend(page)
        while position < len(output) and output[position].isspace():
            position += 1
    return items


def load_releases(path: pathlib.Path, github_states: dict[str, ReleaseState]) -> list[Release]:
    """Returns the releases of the manifest, oldest version first."""
    tags = manifest.Manifest.loads(path.read_text(), str(path)).tags()
    highest = tags[-1][0] if tags else None
    previous_by_track: dict[str, versions.Version] = {}
    releases = []
    for version, entry in tags:
        if not entry.qualified:
            state = ReleaseState.DRAFT
        elif version == highest:
            state = ReleaseState.LATEST
        else:
            state = ReleaseState.REGULAR
        releases.append(
            Release(
                version=version,
                state=state,
                github_state=github_states.get(version.tag),
                date=entry.date,
                github_commit=entry.github_commit,
                release_notes=tuple(entry.release_notes),
                previous=previous_by_track.get(version.track),
            )
        )
        previous_by_track[version.track] = version
    return releases


def list_tags() -> dict[str, str]:
    """Returns {tag: commit} for every release tag of the repository, annotated tags peeled."""
    tags = {}
    for ref in gh_api_list(f"repos/{REPO}/git/matching-refs/tags/{versions.TAG_PREFIX}"):
        tag = ref["ref"].removeprefix("refs/tags/")
        if ref["object"]["type"] == "tag":
            tags[tag] = gh_api(f"repos/{REPO}/git/tags/{ref['object']['sha']}")["object"]["sha"]
        else:
            tags[tag] = ref["object"]["sha"]
    return tags


def latest_release() -> str | None:
    result = subprocess.run(["gh", "api", f"repos/{REPO}/releases/latest"], capture_output=True, text=True)
    return json.loads(result.stdout)["tag_name"] if result.returncode == 0 else None


def list_releases() -> dict[str, ReleaseState]:
    """Returns {tag: state} for every release, drafts included."""
    latest = latest_release()
    states = {}
    for release in gh_api_list(f"repos/{REPO}/releases"):
        tag = release["tag_name"]
        if release["draft"]:
            states[tag] = ReleaseState.DRAFT
        elif tag == latest:
            states[tag] = ReleaseState.LATEST
        else:
            states[tag] = ReleaseState.REGULAR
    return states


def commit_exists(commit: str) -> bool:
    return subprocess.run(["gh", "api", f"repos/{REPO}/commits/{commit}"], capture_output=True).returncode == 0


@dataclasses.dataclass(frozen=True)
class Plan:
    """The delta between GitHub and the manifest"""

    # Releases whose git tag does not exist on GitHub yet, created before any release.
    missing_tags: list[Release]
    draft: list[Release]
    regular: list[Release]
    latest: Release | None

    def empty(self) -> bool:
        return not (self.missing_tags or self.releases())

    def releases(self) -> list[Release]:
        """The releases to create or update, in the order they are applied: latest goes last, nothing can replace it."""
        return self.draft + self.regular + ([self.latest] if self.latest else [])

    def format(self, current_latest: str | None) -> str:
        """Renders the plan like 'terraform plan', current_latest is the latest release of GitHub before it.

        Only the changes are rendered, no logs, so the result can be published as is, e.g. in a CL
        description proposing the manifest: what happens to GitHub once it is merged.
        """
        if self.empty():
            return f"No changes. {REPO} is up to date with the manifest.\n"
        # (sign, tag, state before, state after), '+' creates, '~' updates in place.
        changes = []
        for release in self.releases():
            before = release.github_state.value if release.github_state else "none"
            changes.append(("~" if release.github_state else "+", release.tag, before, release.state.value))
        if self.latest and current_latest and current_latest != self.latest.tag:
            # Not applied but done by GitHub, a single release is the latest one.
            changes.append(("~", current_latest, "latest", f"regular (replaced by {self.latest.tag})"))
        tag_width = max(len(tag) for tag in [*(release.tag for release in self.missing_tags), *(c[1] for c in changes)])
        before_width = max((len(row[2]) for row in changes), default=0)
        lines = [f"{REPO} will be changed as follows:", ""]
        for release in self.missing_tags:
            lines.append(f"  + tag      {release.tag:<{tag_width}}  at {release.github_commit}")
        for sign, tag, before, after in changes:
            lines.append(f"  {sign} release  {tag:<{tag_width}}  {before:<{before_width}} -> {after}")
        created = sum(1 for row in changes if row[0] == "+")
        lines += [
            "",
            f"Plan: {len(self.missing_tags)} tags to create, {created} releases to create, "
            f"{len(changes) - created} releases to change.",
        ]
        return "\n".join(lines) + "\n"


def make_plan(releases: list[Release], existing_tags: dict[str, str]) -> Plan:
    missing_tags, draft, regular, latest = [], [], [], None
    for release in releases:
        if release.tag not in existing_tags:
            if not commit_exists(release.github_commit):
                raise versions.ErrorOccurred(f"{release.tag}: commit {release.github_commit} does not exist in {REPO}")
            missing_tags.append(release)

        if release.github_state is release.state:
            continue
        match release.state:
            case ReleaseState.DRAFT:
                if release.github_state is None:
                    draft.append(release)
                else:
                    print(f">>> Warning: release {release.tag} is published but not qualified, it is left as it is.")
            case ReleaseState.REGULAR:
                # The latest release of GitHub is never updated to regular: it stays latest until
                # another release takes its place, GitHub would pick a latest release of its own.
                if release.github_state is not ReleaseState.LATEST:
                    regular.append(release)
            case ReleaseState.LATEST:
                latest = release
    return Plan(missing_tags=missing_tags, draft=draft, regular=regular, latest=latest)
