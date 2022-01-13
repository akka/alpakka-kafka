# Welcome! Thank you for contributing to the Alpakka Kafka connector!

We follow the standard GitHub [fork & pull](https://help.github.com/articles/using-pull-requests/#fork--pull) approach to pull requests. Just fork the official repo, develop in a branch, and submit a PR!

You're always welcome to submit your PR straight away and start the discussion (without reading the rest of this wonderful doc, or the README.md). The goal of these notes is to make your experience contributing to Alpakka as smooth and pleasant as possible. We're happy to guide you through the process once you've submitted your PR.

# The Akka Community

Please check out [Get Involved](https://akka.io/get-involved/).

# Contributing to Alpakka Kafka

## General Workflow

This is the process for committing code into master.

1. Make sure you have signed the Lightbend CLA, if not, [sign it online](http://www.lightbend.com/contribute/cla).

1. To avoid duplicated effort, it might be good to check the [issue tracker](https://github.com/akka/alpakka/issues) and [existing pull requests](https://github.com/akka/alpakka/pulls) for existing work.
   - If there is no ticket yet, feel free to [create one](https://github.com/akka/alpakka/issues/new) to discuss the problem and the approach you want to take to solve it.

1. Perform your work according to the [pull request requirements](#pull-request-requirements).

1. When the feature or fix is completed you should open a [Pull Request](https://help.github.com/articles/using-pull-requests) on [GitHub](https://github.com/akka/alpakka/pulls). 

1. The Pull Request should be reviewed by other maintainers (as many as feasible/practical). Note that the maintainers can consist of outside contributors, both within and outside Lightbend. Outside contributors are encouraged to participate in the review process, it is not a closed process.

1. After the review you should fix the issues (review comments, CI failures) by pushing a new commit for new review, iterating until the reviewers give their thumbs up and CI tests pass.

1. When the branch conflicts with its merge target (either by way of git merge conflict or failing CI tests), do **not** merge the target branch into your feature branch. Instead rebase your branch onto the target branch and update it with `git push -f`.

## Pull Request Requirements

For a Pull Request to be considered at all it has to meet these requirements:

1. Pull Request branch should be given a unique descriptive name that explains its intent. 

1. Refer to issues it intends to fix by adding "Fixes #{issue id}" to the notes.

1. Code in the branch should live up to the current code standard:
   - Not violate [DRY](https://www.oreilly.com/library/view/97-things-every/9780596809515/ch30.html).
   - [Boy Scout Rule](https://www.oreilly.com/library/view/97-things-every/9780596809515/ch08.html) needs to have been applied.

1. Regardless if the code introduces new features or fixes bugs or regressions, it must have comprehensive tests.

1. The code must be well documented in the Lightbend's standard documentation format (see the [Documentation](documentation) section below).

1. The commit messages must properly describe the changes, see [further below](#creating-commits-and-writing-commit-messages).

1. Do not use ``@author`` tags since it does not encourage [Collective Code Ownership](http://www.extremeprogramming.org/rules/collective.html). Contributors get the credit they deserve in the release notes.

If these requirements are not met then the code should **not** be merged into master, or even reviewed - regardless of how good or important it is. No exceptions.


## Documentation

Using [Paradox](https://github.com/lightbend/paradox) syntax (which is very close to markdown), create or complement the documentation in the `docs` module.
Prepare code snippets to be integrated by Paradox in the tests. Such example should be part of real tests and not in unused methods.

Use ScalaDoc if you see the need to describe the API usage better than the naming does.

Run `sbt docs/paradox` to generate reference docs while developing. Generated documentation can be 
found in the `./docs/target/paradox/site/main` directory.


## External Dependencies

All the external runtime dependencies for the project, including transitive dependencies, must have an open source license that is equal to, or compatible with, [Apache 2](http://www.apache.org/licenses/LICENSE-2.0).

Which licenses are compatible with Apache 2 are defined in [this doc](http://www.apache.org/legal/3party.html#category-a), where you can see that the licenses that are listed under ``Category A`` automatically compatible with Apache 2, while the ones listed under ``Category B`` needs additional action:

> Each license in this category requires some degree of [reciprocity](http://www.apache.org/legal/3party.html#define-reciprocal); therefore, additional action must be taken in order to minimize the chance that a user of an Apache product will create a derivative work of a reciprocally-licensed portion of an Apache product without being aware of the applicable requirements.

Dependency licenses will be checked automatically by [FOSSA](https://fossa.com/).


## Work In Progress

It is ok to work on a public feature branch in the GitHub repository. Something that can sometimes be useful for early feedback etc. If so then it is preferable to name the branch accordingly. This can be done by either prefix the name with ``wip-`` as in ‘Work In Progress’, or use hierarchical names like ``wip/..``, ``feature/..`` or ``topic/..``. Either way is fine as long as it is clear that it is work in progress and not ready for merge. This work can temporarily have a lower standard. However, to be merged into master it will have to go through the regular process outlined above, with Pull Request, review etc..

Also, to facilitate both well-formed commits and working together, the ``wip`` and ``feature``/``topic`` identifiers also have special meaning.   Any branch labelled with ``wip`` is considered “git-unstable” and may be rebased and have its history rewritten.   Any branch with ``feature``/``topic`` in the name is considered “stable” enough for others to depend on when a group is working on a feature.


## Creating Commits And Writing Commit Messages

Follow these guidelines when creating public commits and writing commit messages.

1. First line should be a descriptive sentence what the commit is doing. It should be possible to fully understand what the commit does — but not necessarily how it does it — by just reading this single line. We follow the “imperative present tense” style for commit messages ([more info here](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html)).

   It is **not ok** to only list the ticket number, type "minor fix" or similar.
   If the commit is a small fix, then you are done. If not, go to 3.

1. Following the single line description should be a blank line followed by an enumerated list with the details of the commit.

1. Add keywords for your commit (depending on the degree of automation we reach, the list may change over time):
    * ``Review by @gituser`` - if you want to notify someone on the team. The others can, and are encouraged to participate.

Example:

    Add eventsByTag query #123

    * Details 1
    * Details 2
    * Details 3


## How To Enforce These Guidelines?

1. [GitHub Actions](https://github.com/akka/alpakka-kafka/actions) automatically merges the code, builds it, runs the tests and sets Pull Request status accordingly of results in GitHub.
1. [Scalafmt](http://scalameta.org/scalafmt/) enforces some of the code style rules.
1. [sbt-header plugin](https://github.com/sbt/sbt-header) manages consistent copyright headers in every source file.
1. A GitHub bot checks whether you've signed the Lightbend CLA. 

