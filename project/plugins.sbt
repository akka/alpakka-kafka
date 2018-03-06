resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.7")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.0")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.0.0")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.5.0")

addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.3.2")

addSbtPlugin("com.lightbend" % "sbt-whitesource"  % "0.1.10")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.3")
