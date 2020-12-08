import sbt._
import sbt.Keys._
import sbtwhitesource.WhiteSourcePlugin.autoImport._
import sbtwhitesource._
import scala.sys.process.Process
import scala.util.Try

object Whitesource extends AutoPlugin {
  private lazy val gitCurrentBranch =
    Try(Process("git rev-parse --abbrev-ref HEAD").!!.replaceAll("\\s", "")).recover {
      case e => sys.error(s"Couldn't determine git branch for Whitesource: $e")
    }.toOption

  override def requires = WhiteSourcePlugin

  override def trigger = allRequirements

  override lazy val projectSettings = Seq(
    // do not change the value of whitesourceProduct
    whitesourceProduct := "Lightbend Reactive Platform",
    whitesourceAggregateProjectName := {
      (moduleName in LocalRootProject).value + "-" + (
        if (isSnapshot.value)
          if (gitCurrentBranch.contains("master")) "master"
          else "adhoc"
        else majorMinor((version in LocalRootProject).value).map(_ + "-stable").getOrElse("adhoc")
      )
    },
    whitesourceForceCheckAllDependencies := true,
    whitesourceFailOnError := true
  )

  private def majorMinor(version: String): Option[String] = """\d+\.\d+""".r.findFirstIn(version)
}
