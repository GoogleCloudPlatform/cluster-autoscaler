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

Creates a lightweight tag at the GitHub commit and a release for every entry
that does not exist yet, with the release notes recorded in the manifest.
Existing tags are never moved.
"""

import argparse
import pathlib
import subprocess
import sys
import urllib.error

import manifest
import releases
import versions

RELEASE_BODY = """\
GKE Cluster Autoscaler {tag}

- Released: {date}
- Commit: {commit}

## Release notes

{notes}
"""
NO_RELEASE_NOTES = "No user facing changes."
REPO = releases.REPO


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--manifest",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parents[2] / manifest.PATH,
        help="Manifest to publish (default: %(default)s)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the changes without asking to apply them")
    parser.add_argument("--yes", action="store_true", help="Apply the changes without asking, e.g. in CI")
    return parser.parse_args()


def gh(*args: str) -> str:
    return subprocess.run(["gh", *args], capture_output=True, text=True, check=True).stdout


def format_release_body(release: releases.Release) -> str:
    notes = "\n".join(f"- {note}" for note in release.release_notes) or NO_RELEASE_NOTES
    return RELEASE_BODY.format(tag=release.tag, date=release.date, commit=release.github_commit, notes=notes)


def state_flags(state: releases.ReleaseState) -> list[str]:
    # Drafts are never marked as latest, GitHub refuses it.
    draft = "true" if state is releases.ReleaseState.DRAFT else "false"
    latest = "true" if state is releases.ReleaseState.LATEST else "false"
    return [f"--draft={draft}", f"--latest={latest}"]


def create_tag(release: releases.Release) -> None:
    gh(
        "api",
        "--method",
        "POST",
        f"repos/{REPO}/git/refs",
        "--field",
        f"ref=refs/tags/{release.tag}",
        "--field",
        f"sha={release.github_commit}",
    )


def create_release(release: releases.Release) -> None:
    cmd = ["release", "create", release.tag, "--repo", REPO, "--verify-tag", "--title", release.tag]
    cmd += ["--notes", format_release_body(release), "--generate-notes"]
    if release.previous:
        cmd += ["--notes-start-tag", release.previous.tag]
    gh(*cmd, *state_flags(release.state))


def update_release(release: releases.Release) -> None:
    gh("release", "edit", release.tag, "--repo", REPO, *state_flags(release.state))


def confirm(assume_yes: bool) -> bool:
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        raise versions.ErrorOccurred("confirmation needs a terminal, pass --yes to proceed without one")
    return input(">>> Proceed? [y/N] ").strip().lower() in ("y", "yes")


def apply_plan(plan: releases.Plan) -> None:
    for release in plan.missing_tags:
        create_tag(release)
        print(f">>> Created tag {release.tag}.")
    for release in plan.releases():
        if release.github_state is None:
            create_release(release)
            print(f">>> Created {release.state.value} release {release.tag}.")
        else:
            update_release(release)
            print(f">>> Updated release {release.tag}: {release.github_state.value} -> {release.state.value}.")


def main() -> None:
    args = parse_args()

    plan, plan_text = releases.plan_manifest(manifest.Manifest.loads(args.manifest.read_text(), str(args.manifest)))
    print(plan_text, end="")
    if plan.empty():
        return
    if args.dry_run:
        print(">>> Dry run, nothing was changed.")
        return
    if not confirm(args.yes):
        print(">>> Aborted, nothing was changed.")
        return
    apply_plan(plan)


if __name__ == "__main__":
    try:
        main()
    except versions.ErrorOccurred as e:
        print(f">>> Error: {e}", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError as e:
        print(f">>> Error: '{' '.join(map(str, e.cmd))}' failed with exit code {e.returncode}", file=sys.stderr)
        if e.stderr:
            print(e.stderr.strip(), file=sys.stderr)
        sys.exit(1)
    except urllib.error.HTTPError as e:
        print(f">>> Error: {e.url} failed with HTTP {e.code} {e.reason}", file=sys.stderr)
        sys.exit(1)
