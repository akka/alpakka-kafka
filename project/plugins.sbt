resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.1")

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.0.0")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.4.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.13.0")

addSbtPlugin("de.heikoseeberger" % "sbt-header" % "1.5.1")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.4.0")

addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.2.9")

addSbtPlugin("com.lightbend" % "sbt-whitesource"  % "0.1.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.9.3")
