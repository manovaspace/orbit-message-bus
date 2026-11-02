# Releasing orbit-message-bus

`orbit-message-bus` is a Go library. It has **no container image and no deploy step**: a release ends
at the git tag and its GitHub release notes.

## How a release happens

Write Conventional Commit messages and merge them into `main` through a pull request. On every push
to `main`, the `release-please` workflow opens or updates one bot release pull request titled
`chore(main): release X.Y.Z`. That pull request runs the ordinary CI checks. When they are green and
the pull request is reviewed, a maintainer merges it, and the same workflow creates the `vX.Y.Z` tag
and the GitHub release with the generated notes. Nobody bumps a version or pushes a tag by hand.

The release branch (`release-please--branches--main`) exists only while the pull request is open.
There is no long-lived release branch and no demo branch (ADR-044 D2).

## Conventional Commit types and what bumps

| Commit | Bump |
| --- | --- |
| `feat:` / `feature:` | MINOR |
| `fix:`, `perf:`, `refactor:`, `revert:` | PATCH |
| `!` after the type or scope (`feat!:`, `fix(api)!:`), or a `BREAKING CHANGE:` footer | MAJOR |
| `chore:`, `docs:`, `ci:`, `test:`, `style:`, `build:` | none |
| any type not listed above | none |

`refactor:` bumps a patch because `release-please-config.json` lists it as a visible changelog
section; the release-please defaults would hide it and release nothing. A release pull request
appears only when at least one merged commit has a visible section, so a docs-only or CI-only change
does not create one. Squash titles are the commit subject on `main`, so the pull request title must
also be a Conventional Commit.

Until the library reaches 1.0.0, `bump-minor-pre-major` makes a breaking change bump MINOR instead
of MAJOR, so the bot never proposes 1.0.0 on its own; `bump-patch-for-minor-pre-major` is off, so a
`feat:` still bumps MINOR. Reaching 1.0.0 is a deliberate change to `release-please-config.json`.

No `v*` tag existed when the bot was configured, so `.release-please-manifest.json` starts at
`0.0.0`. The first release pull request proposes `0.1.0` because the configuration sets
`initial-version` to `0.1.0`; later bumps follow the table.

## How to read the release pull request

- **Title** — `chore(main): release X.Y.Z`.
- **Branch** — `release-please--branches--main`.
- **Label** — `autorelease: pending` until the release is tagged.
- **Files** — only `CHANGELOG.md` (a new version section) and `.release-please-manifest.json` (the
  new version). Any other changed file means the release pull request is wrong.
- **Body** — the commits and the notes for the proposed version. It updates as new commits land on
  `main`, and the proposed version grows with them.

Never edit the release pull request by hand except to resolve a genuine conflict in `CHANGELOG.md`.
Land the conflicting change on `main` and let the bot rebuild the pull request. The hand-written
`[Unreleased]` section that predates the bot stays as history; release-please prepends its own
sections above the old ones and owns the file from then on.

## The release pull request checks

This repository has **no release-tier workflow**: there is no `.github/workflows/release-check.yml`,
so the release pull request runs only `.github/workflows/ci.yml`, the ordinary `manovaspace/ci` Go
check on the pull-request tier. Add a release-check workflow in a separate change if the library
needs service-container conformance tests before a release.

Do not merge while a check is red or pending. `o pr merge` refuses in both cases (ADR-037).

## What merging does

The merge push runs `release-please` again. It creates the tag `vX.Y.Z` on `main` and publishes a
GitHub release whose notes come from the `CHANGELOG.md` entry, then relabels the pull request
`autorelease: tagged`. Consumers can then fetch the module:

```text
go get github.com/manovaspace/orbit-message-bus@vX.Y.Z
```

No image is built and nothing is deployed, because this repository is a library.

## Rollback

Do not move or force-push a published tag. If a release is wrong, revert the change through a `fix:`
or `revert:` pull request and let the bot publish the next patch.

If a tag should never have existed and no consumer could have fetched it, the owner may delete the
GitHub release and the tag, then revert the release commit on `main` through a pull request. Deleting
a tag is destructive and needs the owner.

To withdraw a bad version while keeping the tag, add a `retract` directive to `go.mod` and release
the retraction. Go module caches keep serving a retracted version until consumers refresh, and
`retract` is advisory.

## Who approves

The bot never merges its own pull request. A maintainer reviews the release pull request and merges
it with `o pr merge`, which refuses while checks fail or are pending (ADR-037). Where the public
`main-protection` ruleset is installed it requires the CI check but no approval count, so the human
review is a convention, not a server-side gate.

## Secrets used

Names only — never put a value in this file.

| Secret | Used by | Purpose |
| --- | --- | --- |
| `RELEASE_TOKEN` | `release-please` workflow | Personal access token so the release pull request triggers its own checks |
| `GH_READ_TOKEN` | CI | Read access to private `github.com/manovaspace` modules |
| `NPM_TOKEN` | CI | Read access to the `@orbit` npm scope |

If `RELEASE_TOKEN` is empty, the `release-please` workflow prints `RELEASE_TOKEN not set, skipping`
and succeeds without opening a release pull request.
