# Releasing

Create a new issue from the [Alpakka Kafka Release Train Issue Template](docs/release-train-issue-template.md) and follow the steps.

```bash
~/alpakka> scripts/create-release-issue.sh `version-to-be-released`
```

### Releasing only updated docs

It is possible to release a revised documentation to the already existing release.

1. Create a new branch from a release tag. If a revised documentation is for the `v1.1.1` release, then the name of the new branch should be `docs/v1.1.1`.
1. Add and commit `version.sbt` file that pins the version to the one, that is being revised. Also set `isSnapshot` to `false` for the stable documentation links. For example:
    ```scala
    ThisBuild / version := "1.1.1"
    ThisBuild / isSnapshot := false
    ```
1. Make all of the required changes to the documentation.
1. Check the documentation update locally:
    ```sh
    sbt docs/previewSite
    ```
1. If the generated documentation looks good, send it to Gustav:
    ```sh
    sbt docs/publishRsync
    ```
1. Do not forget to push the new branch back to GitHub.
