addSbtPlugin("com.dwijnand" % "sbt-dynver" % "4.1.1")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.5.11")
addSbtPlugin("net.aichler" % "sbt-jupiter-interface" % "0.11.1")
// discipline
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.9.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.0")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.1")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.7.0")
// docs
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.45")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.2")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.2")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.5.0-RC2")

resolvers += Resolver.jcenterRepo
