"""Write the package index for the wheels on akayu's GitHub releases.

The docs workflow runs it after building the site, so the index is served with
the docs, at https://isaactay.github.io/akayu/simple/. pip and uv read it as a
package index (the "simple" repository API, PEP 503):

    uv add akayu --index akayu=https://isaactay.github.io/akayu/simple/
    pip install akayu --extra-index-url https://isaactay.github.io/akayu/simple/

Each link carries the file's sha256: pip checks it on every install, and uv
records it in uv.lock and checks it on every sync. (Releases are immutable,
so a released file can't change anyway.)

usage:
    scripts/wheel_index.py SITE_DIR                        # every published release's files
    scripts/wheel_index.py SITE_DIR --files DIR --base-url URL   # DIR's files, served at URL

GITHUB_REPOSITORY names the repository (the workflows set it), and GH_TOKEN,
if set, authenticates the API requests.
"""

import argparse
import hashlib
import html
import json
import os
import re
import urllib.request
from pathlib import Path

DIST_FILE = re.compile(r"^akayu-[^/]+\.(?:whl|tar\.gz)$")


def assets_from_releases(releases):
    """(name, url, sha256 or None) for each file of the published releases
    in a page of the GitHub API's release list."""
    files = []
    for release in releases:
        if release["draft"]:
            continue
        for asset in release["assets"]:
            digest = asset.get("digest") or ""
            sha = digest.removeprefix("sha256:") if digest.startswith("sha256:") else None
            files.append((asset["name"], asset["browser_download_url"], sha))
    return files


def release_files(repo, token=None):
    """(name, url, sha256 or None) for each file on the repository's releases."""
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    files, page = [], 1
    while True:
        url = f"https://api.github.com/repos/{repo}/releases?per_page=100&page={page}"
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as response:
            releases = json.load(response)
        files += assets_from_releases(releases)
        if len(releases) < 100:
            return files
        page += 1


def local_files(directory, base_url):
    """(name, url, sha256) for each file in `directory`, served at `base_url`."""
    return [
        (p.name, f"{base_url.rstrip('/')}/{p.name}", hashlib.sha256(p.read_bytes()).hexdigest())
        for p in sorted(directory.iterdir())
        if p.is_file()
    ]


def _page(title, links):
    body = "".join(f'    <a href="{html.escape(href)}">{html.escape(text)}</a><br>\n' for text, href in links)
    return (
        "<!DOCTYPE html>\n<html>\n  <head>\n"
        '    <meta name="pypi:repository-version" content="1.0">\n'
        f"    <title>{html.escape(title)}</title>\n  </head>\n  <body>\n{body}  </body>\n</html>\n"
    )


def write_index(site, files):
    """Write `site`/simple/ listing akayu's wheels and sdists among `files`;
    returns how many it lists."""
    links = sorted((name, f"{url}#sha256={sha}" if sha else url) for name, url, sha in files if DIST_FILE.match(name))
    project = Path(site) / "simple" / "akayu"
    project.mkdir(parents=True, exist_ok=True)
    (project.parent / "index.html").write_text(_page("Simple index", [("akayu", "akayu/")]), encoding="utf-8")
    (project / "index.html").write_text(_page("Links for akayu", links), encoding="utf-8")
    return len(links)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("site", type=Path, help="the built site to add simple/ to")
    parser.add_argument("--files", type=Path, help="index this directory's files instead of the releases'")
    parser.add_argument("--base-url", help="where --files' files are served")
    args = parser.parse_args()
    if args.files:
        if not args.base_url:
            parser.error("--files needs --base-url")
        files = local_files(args.files, args.base_url)
    else:
        files = release_files(os.environ.get("GITHUB_REPOSITORY", "IsaacTay/akayu"), os.environ.get("GH_TOKEN"))
    count = write_index(args.site, files)
    print(f"{args.site / 'simple'}: {count} files")


if __name__ == "__main__":
    main()
