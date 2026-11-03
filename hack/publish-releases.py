#!/usr/bin/env python3
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
"""Publishes the releases listed in releases.json as GitHub tags and releases.

Every entry of the manifest records the internal (gerrit_commit) and the GitHub
(github_commit) commit of a release tag, the manifest is rejected if any entry
lacks either. Creates a lightweight tag at the GitHub commit and a release for
every entry that does not exist yet. Releases are created as drafts unless
--no-draft is given. Existing tags are never moved and existing releases are
never modified. Only the highest version in the manifest may become the latest
release.

Release notes are compiled from the 'RELEASE_NOTES=<description>' annotations
of the commits since the previous release of the same minor. Commits annotated
with 'RELEASE_NOTES=NONE' or not annotated at all are left out.

Requirements: gh (authenticated)
"""

import argparse
import dataclasses
import json
import os
import pathlib
import re
import subprocess
import sys

SUPPORTED_SCHEMA = 1
RELEASE_TAG_PATTERN = re.compile(r"^cluster-autoscaler-v(\d+)\.(\d+)\.(\d+)$")
RELEASE_NOTE_PREFIX = "RELEASE_NOTES="
RELEASE_NOTE_NONE = "NONE"

RELEASE_BODY = """\
GKE Cluster Autoscaler {tag}

- Released: {date}
- Commit: {commit}

## Release notes

{notes}
"""
NO_RELEASE_NOTES = "No user facing changes."


class ErrorOccurred(Exception):
    pass


@dataclasses.dataclass(frozen=True)
class Release:
    tag: str
    version: tuple  # (major, minor, patch)
    gerrit_commit: str
    github_commit: str
    date: str

    @property
    def minor(self):
        return self.version[0]


@dataclasses.dataclass(frozen=True)
class Commit:
    sha: str
    message: str


@dataclasses.dataclass(frozen=True)
class ReleaseNote:
    sha: str
    text: str


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--manifest", type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1] / "releases.json",
                        help="Manifest to publish (default: %(default)s)")
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"),
                        help="GitHub repository as OWNER/NAME (default: $GITHUB_REPOSITORY)")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be created without creating anything")
    # TODO: For now all the releases are created as drafts, switch the default
    # to --no-draft once the export flow is stable.
    parser.add_argument("--draft", action=argparse.BooleanOptionalAction, default=True,
                        help="Create releases as drafts to be published manually (default: %(default)s)")
    args = parser.parse_args()
    if not args.repo:
        parser.error("--repo is required when $GITHUB_REPOSITORY is not set")
    return args


def gh(*args):
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=True).stdout


def gh_api(endpoint):
    return json.loads(gh("api", endpoint))


def gh_api_pages(endpoint):
    """Returns the decoded response of every page, 'gh api --paginate' prints them back to back."""
    output = gh("api", "--paginate", endpoint).strip()
    pages, decoder, position = [], json.JSONDecoder(), 0
    while position < len(output):
        page, position = decoder.raw_decode(output, position)
        pages.append(page)
        while position < len(output) and output[position].isspace():
            position += 1
    return pages


def gh_api_list(endpoint):
    return [item for page in gh_api_pages(endpoint) for item in page]


def load_manifest(path):
    """Returns the releases of the manifest in version order, every entry must carry both commits."""
    manifest = json.loads(path.read_text())
    if manifest.get("schema") != SUPPORTED_SCHEMA:
        raise ErrorOccurred(f"{path}: unsupported schema {manifest.get('schema')!r}, expected {SUPPORTED_SCHEMA}")
    releases = []
    for minor in manifest["releases"].values():
        for tag, info in minor["tags"].items():
            match = RELEASE_TAG_PATTERN.match(tag)
            if not match:
                raise ErrorOccurred(f"{path}: tag {tag!r} does not match {RELEASE_TAG_PATTERN.pattern}")
            for key in ("gerrit_commit", "github_commit", "date"):
                if not info.get(key):
                    raise ErrorOccurred(f"{path}: tag {tag!r} has no {key}, it must not be in the manifest")
            releases.append(Release(tag, tuple(map(int, match.groups())),
                                    info["gerrit_commit"], info["github_commit"], info["date"]))
    return sorted(releases, key=lambda release: release.version)


def list_tags(repo):
    """Returns {tag: commit} for every release tag of the repository, annotated tags peeled."""
    tags = {}
    for ref in gh_api_list(f"repos/{repo}/git/matching-refs/tags/cluster-autoscaler-v"):
        tag = ref["ref"].removeprefix("refs/tags/")
        if ref["object"]["type"] == "tag":
            tags[tag] = gh_api(f"repos/{repo}/git/tags/{ref['object']['sha']}")["object"]["sha"]
        else:
            tags[tag] = ref["object"]["sha"]
    return tags


def list_releases(repo):
    """Returns the tags that already have a release, drafts included."""
    return {release["tag_name"] for release in gh_api_list(f"repos/{repo}/releases")}


def commit_exists(repo, commit):
    return subprocess.run(["gh", "api", f"repos/{repo}/commits/{commit}"], capture_output=True).returncode == 0


def list_commits(repo, release, previous):
    """Returns the commits since the previous release, or just the tagged commit without one."""
    if previous is None:
        commit = gh_api(f"repos/{repo}/commits/{release.github_commit}")
        return [Commit(commit["sha"], commit["commit"]["message"])]
    pages = gh_api_pages(f"repos/{repo}/compare/{previous.github_commit}...{release.github_commit}?per_page=100")
    return [Commit(commit["sha"], commit["commit"]["message"]) for page in pages for commit in page["commits"]]


def collect_release_notes(commits):
    """Returns one note per 'RELEASE_NOTES=<text>' line of the commit messages."""
    notes = []
    for commit in commits:
        for line in commit.message.splitlines():
            if not line.startswith(RELEASE_NOTE_PREFIX):
                continue
            text = line.removeprefix(RELEASE_NOTE_PREFIX).strip()
            if text and text.upper() != RELEASE_NOTE_NONE:
                notes.append(ReleaseNote(commit.sha, text))
    return notes


def format_release_body(release, notes):
    bullets = "\n".join(f"- {note.text} ({note.sha[:12]})" for note in notes) or NO_RELEASE_NOTES
    return RELEASE_BODY.format(tag=release.tag, date=release.date, commit=release.github_commit, notes=bullets)


def create_tag(repo, release):
    gh("api", "--method", "POST", f"repos/{repo}/git/refs",
       "--field", f"ref=refs/tags/{release.tag}", "--field", f"sha={release.github_commit}")


def create_release(repo, release, previous, body, latest, draft):
    cmd = ["release", "create", release.tag, "--repo", repo, "--verify-tag", "--title", release.tag,
           "--notes", body, "--generate-notes"]
    if previous:
        cmd += ["--notes-start-tag", previous.tag]
    if draft:
        cmd += ["--draft"]
    # GitHub refuses to mark drafts as latest, they become latest when published.
    if not latest:
        cmd += ["--latest=false"]
    elif not draft:
        cmd += ["--latest=true"]
    gh(*cmd)


def main():
    args = parse_args()
    action = "Would create" if args.dry_run else "Creating"

    releases = load_manifest(args.manifest)
    print(f">>> {args.manifest}: {len(releases)} releases.")
    existing_tags = list_tags(args.repo)
    existing_releases = list_releases(args.repo)
    print(f">>> {args.repo}: {len(existing_tags)} release tags, {len(existing_releases)} releases.")

    conflicts = [release for release in releases
                 if release.tag in existing_tags and existing_tags[release.tag] != release.github_commit]
    for release in conflicts:
        print(f">>> Conflict: {release.tag} exists at {existing_tags[release.tag]}, manifest says {release.github_commit}", file=sys.stderr)
    if conflicts:
        raise ErrorOccurred("tags are never moved, resolve the conflicts above manually")

    highest = releases[-1] if releases else None
    previous_by_minor = {}
    created_tags = created_releases = 0
    for release in releases:
        previous = previous_by_minor.get(release.minor)
        previous_by_minor[release.minor] = release

        if release.tag in existing_tags:
            print(f">>> Skipping tag {release.tag}, already exists at {release.github_commit}")
        else:
            if not commit_exists(args.repo, release.github_commit):
                raise ErrorOccurred(f"{release.tag}: commit {release.github_commit} does not exist in {args.repo}")
            print(f">>> {action} tag {release.tag} at {release.github_commit}")
            if not args.dry_run:
                create_tag(args.repo, release)
            created_tags += 1

        if release.tag in existing_releases:
            print(f">>> Skipping release {release.tag}, already exists")
            continue
        commits = list_commits(args.repo, release, previous)
        notes = collect_release_notes(commits)
        since = f" since {previous.tag}" if previous else ""
        kind = "draft release" if args.draft else "release"
        print(f">>> {action} {kind} {release.tag}: {len(commits)} commits{since}, {len(notes)} release notes")
        for note in notes:
            print(f"    - {note.text} ({note.sha[:12]})")
        if not args.dry_run:
            body = format_release_body(release, notes)
            create_release(args.repo, release, previous, body, latest=release is highest, draft=args.draft)
        created_releases += 1

    print(f">>> {'Would create' if args.dry_run else 'Created'} {created_tags} tags and {created_releases} releases.")


if __name__ == "__main__":
    try:
        main()
    except ErrorOccurred as e:
        print(f">>> Error: {e}", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f">>> Error: '{' '.join(map(str, e.cmd))}' failed with exit code {e.returncode}", file=sys.stderr)
        if e.stderr:
            print(e.stderr.strip(), file=sys.stderr)
        sys.exit(1)
