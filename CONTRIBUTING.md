# Contribution guide

Contributions are more than welcome and we're always looking for use cases and feature ideas!

This document helps you get started on:

- [Submitting a pull request](#submitting-a-pull-request)
- [Writing documentation](#writing-documentation)
- [Useful tricks](#useful-tricks)
- [Reporting a bug](#reporting-a-bug)
- [Asking for help](#asking-for-help)



## Submitting a Pull Request

To contribute, [fork](https://help.github.com/articles/fork-a-repo/) Cape Python, commit your changes, and [open a pull request](https://help.github.com/articles/using-pull-requests/).

While you may be asked to make changes to your submission during the review process, we will work with you on this and suggest changes. Consider giving us [push rights to your branch](https://help.github.com/articles/allowing-changes-to-a-pull-request-branch-created-from-a-fork/) so we can potentially also help via commits.

### Commit history and merging

For the sake of transparency our key rule is to keep a logical and intelligible commit history, meaning anyone stepping through the commits on either the `master` branch or as part of a review should be able to easily follow the changes made and their potential implications.

To this end we ask all contributors to sanitize pull requests before submitting them. All pull requests will either be [squashed or rebased](https://help.github.com/en/articles/about-pull-request-merges).

Some guidelines:

- Even simple code changes such as moving code around can obscure semantic changes, and in those case there should be two commits: for example, one that only moves code (with a note of this in the commit description) and one that performs the semantic change.

- Progressions that have no logical justification for being split into several commits should be squeezed.

- Code does not have to compile or pass all tests at each commit, but leave a remark and a plan in the commit description so reviewers are aware and can plan accordingly.

See below for some [useful tricks](#git-and-github) for working with Git and GitHub.

### Before submitting for review

Make sure to give some context and overview in the body of your pull request to make it easier for reviewers to understand your changes. Ideally explain why your particular changes were made the way they are.

Importantly, use [keywords](https://help.github.com/en/articles/closing-issues-using-keywords) such as `Closes #<issue-number>` to indicate any issues or other pull requests related to your work.

Furthermore:

- Run tests (`make test`) and linting (`make lint`) before submitting as our [CI](#continuous-integration) will block pull requests failing either check
- Test your change thoroughly with unit tests where appropriate
- Update any affected docstrings in the code base
- Add a line in [CHANGELOG.md](CHANGELOG.md) for any major change

## Continuous integration

All pull requests are run against our [continuous integration suite](https://github.com/capeprivacy/cape-python/actions). The entire suite must pass before a pull request is accepted.

## Writing Documentation

Ensure you add docstrings where necessary. We use [Google's style](https://github.com/google/styleguide/blob/gh-pages/pyguide.md).

The documentation site is managed in the [documentation repository](https://github.com/capeprivacy/documentation).

## Useful Tricks

### git and GitHub

- [GitHub Desktop](https://desktop.github.com/) provides a useful interface for inspecting and committing code changes
- `git add -p`
  - lets you leave out some changes in a file (GitHub Desktop can be used for this as well)
- `git commit --amend`
  - allows you to add to the previous commit instead of creating a new one
- `git rebase -i <commit>`
  - allows you to [squeeze and reorder commits](https://git-scm.com/book/en/v2/Git-Tools-Rewriting-History)
  - use `HEAD~5` to consider 5 most recent commits
  - use `<hash>~1` to start from commit identified by `<hash>`
- `git rebase master`
  - [pull in latest updates](https://git-scm.com/book/en/v2/Git-Branching-Rebasing) on `master`
- `git fetch --no-tags <repo> <remote branch>:<local branch>`
  - pulls down a remote branch from e.g. a fork and makes it available to check out as a local branch
  - `<repo>` is e.g. `git@github.com:<user>/tf-encrypted.git`
- `git push <repo> <local branch>:<remote branch>`
  - pushes the local branch to a remote branch on e.g. a fork
  - `<repo>` is e.g. `git@github.com:<user>/tf-encrypted.git`
- `git tag -d <tag> && git push origin :refs/tags/<tag>`
  - can be used to delete a tag remotely

## Reporting a Bug

Please file [bug reports](https://github.com/capeprivacy/cape-python/issues/new?template=bug_report.md) as GitHub issues.

### Security disclosures

If you encounter a security issue then please responsibly disclose it by reaching out to us at [privacy@capeprivacy.com](privacy@capeprivacy.com). We will work with you to mitigate the issue and responsibly disclose it to anyone using the project in a timely manner.

## Asking for help

If you have any questions you are more than welcome to reach out through GitHub issues or [our Slack channel](https://join.slack.com/t/capecommunity/shared_invite/zt-f8jeskkm-r9_FD0o4LkuQqhJSa~~IQA).