# Releasing

From a direct clone (rather than a fork). You will need permission in sonatype to push to akka repositories.

* sbt -Dpublish.maven.central=true
  * release skip-tests
* close the staging repo in sonatype (https://oss.sonatype.org/#welcome)
* verify the contents of the staging
* release the staging repo in sonatype
* push to origin including tags
* SCP docs to gustav
  - scp -r docs/target/paradox/site/main/* gustav.akka.io:www/docs/akka-stream-kafka/<new-version>/
  - scp -r core/target/api/* gustav.akka.io:www/api/akka-stream-kafka/<new-version>/
* Update the current links on gustav
  - ln -snf <new-version> current in www/api/akka-stream-kafka/ and www/docs/akka-stream-kafka

If you don't have access to gustav ask someone from the core akka team to do it or
give you access.
