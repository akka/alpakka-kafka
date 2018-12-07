import sbt._

// See https://github.com/sbt/sbt/issues/3618#issuecomment-424924293
// for  "javax.ws.rs" % "javax.ws.rs-api" % "2.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar"),
object PackagingTypePlugin extends AutoPlugin {
  override val buildSettings = {
    sys.props += "packaging.type" -> "jar"
    Nil
  }
}
