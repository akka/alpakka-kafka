resolvers += "Akka library repository".at("https://repo.akka.io/maven")

addSbtPlugin("com.github.sbt" % "sbt-dynver" % "5.0.1")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.6.1")
addSbtPlugin("net.aichler" % "sbt-jupiter-interface" % "0.11.1")
// discipline
addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.10.0")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")
addSbtPlugin("com.typesafe" % "sbt-mima-plugin" % "1.1.4")
addSbtPlugin("com.lightbend.sbt" % "sbt-java-formatter" % "0.8.0")
// docs
addSbtPlugin("io.akka" % "sbt-paradox-akka" % "24.10.6")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox-dependencies" % "0.2.4")
addSbtPlugin("com.lightbend.sbt" % "sbt-publish-rsync" % "0.3")
addSbtPlugin("com.github.sbt" % "sbt-unidoc" % "0.5.0")
addSbtPlugin("com.thoughtworks.sbt-api-mappings" % "sbt-api-mappings" % "3.0.2")
addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.5.0")

resolvers += Resolver.jcenterRepo
