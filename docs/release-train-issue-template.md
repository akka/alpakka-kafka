Release Alpakka Kafka $VERSION$

<!--
# Release Train Issue Template for Alpakka Kafka

(Liberally copied and adopted from Scala itself https://github.com/scala/scala-dev/blob/b11cd2e4a4431de7867db6b39362bea8fa6650e7/notes/releases/template.md)

For every Alpakka Kafka release, make a copy of this file named after the release, and expand the variables.
Ideally replacing variables could become a script you can run on your local machine.

Variables to be expanded in this template:
- $VERSION$=???

Key links:
  - akka/alpakka-kafka milestone: https://github.com/akka/alpakka-kafka/milestone/?
-->
### ~ 1 week before the release

- [ ] Check that any new `deprecated` annotations use the correct version name
- [ ] Check that open PRs and issues assigned to the milestone are reasonable
- [ ] Decide on a planned release date
- [ ] Create a new milestone for the [next version](https://github.com/akka/alpakka-kafka/milestones)
- [ ] Check [closed issues without a milestone](https://github.com/akka/alpakka-kafka/issues?utf8=%E2%9C%93&q=is%3Aissue%20is%3Aclosed%20no%3Amilestone) and either assign them the 'upcoming' release milestone or `invalid/not release-bound`

### 1 day before the release

- [ ] Make sure all important / big PRs have been merged by now
- [ ] Check versions listed in [home.md](https://github.com/akka/alpakka-kafka/blob/master/docs/src/main/paradox/home.md)

### Preparing release notes in the documentation / announcement

- [ ] Review the [draft release notes](https://github.com/akka/alpakka-kafka/releases)
- [ ] For non-patch releases: Create a news item draft PR on [akka.github.com](https://github.com/akka/akka.github.com), using the milestone
- [ ] Move all [unclosed issues](https://github.com/akka/alpakka-kafka/issues?q=is%3Aopen+is%3Aissue+milestone%3A$VERSION$) for this milestone to the next milestone
- [ ] Release notes PR has been merged
- [ ] Close the [$VERSION$ milestone](https://github.com/akka/alpakka-kafka/milestones?direction=asc&sort=due_date)

### Cutting the release

- [ ] Wait until [master build finished](https://github.com/akka/alpakka-kafka/actions?query=branch%3Amaster) after merging the release notes 
- [ ] Update the [draft release](https://github.com/akka/alpakka-kafka/releases) with the next tag version `v$VERSION$`, title and release description linking to the announcement, release notes and milestone
- [ ] Check that GitHub Actions release build has executed successfully (GitHub Actions will start a [CI build](https://github.com/akka/alpakka-kafka/actions) for the new tag and publish artifacts to Sonatype)

### Check availability
- [ ] Check [API](https://doc.akka.io/api/alpakka-kafka/$VERSION$/) documentation
- [ ] Check [reference](https://doc.akka.io/docs/alpakka-kafka/$VERSION$/) documentation
- [ ] Check the release on [Maven central](https://repo1.maven.org/maven2/com/typesafe/akka/akka-stream-kafka_2.13/$VERSION$/)

### When everything is on maven central
- [ ] Log into `gustav.akka.io` as `akkarepo`
  - [ ] update the `current` links on `repo.akka.io` to point to the latest version with
     ```
     ln -nsf $VERSION$ www/docs/alpakka-kafka/current
     ln -nsf $VERSION$ www/api/alpakka-kafka/current
     ln -nsf $VERSION$ www/docs/alpakka-kafka/2.0
     ln -nsf $VERSION$ www/api/alpakka-kafka/2.0
     ```
  - [ ] check changes and commit the new version to the local git repository
     ```
     cd ~/www
     git add docs/alpakka-kafka/2.0 docs/alpakka-kafka/current docs/alpakka-kafka/$VERSION$
     git add api/alpakka-kafka/2.0 api/alpakka-kafka/current api/alpakka-kafka/$VERSION$
     git commit -m "Alpakka Kafka $VERSION$"
     ```
     
### Announcements
- [ ] For non-patch releases: Merge draft news item for [akka.io](https://github.com/akka/akka.github.com)
- [ ] Send a release notification to [Lightbend discuss](https://discuss.akka.io)
- [ ] Tweet using the akkateam account (or ask someone to) about the new release
- [ ] Announce internally

### Afterwards
- [ ] Update Alpakka Kafka dependency in Alpakka main repository
- [ ] Update version for [Lightbend Supported Modules](https://developer.lightbend.com/docs/lightbend-platform/introduction/getting-help/build-dependencies.html#_alpakka_kafka) in [private project](https://github.com/lightbend/lightbend-platform-docs/blob/master/docs/modules/getting-help/examples/build.sbt)
- Close this issue
