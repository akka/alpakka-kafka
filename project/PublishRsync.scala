import com.typesafe.sbt.site.SitePlugin
import sbt._
import scala.sys.process._

trait PublishRsyncKeys {
  val publishRsyncArtifact = taskKey[(File, String)]("File or directory and a path to publish to")
  val publishRsyncHost = settingKey[String]("hostname to publish to")
  val publishRsync = taskKey[Unit]("Deploy using rsync")
}

object PublishRsyncPlugin extends AutoPlugin {

  override def requires = SitePlugin
  override def trigger = noTrigger

  object autoImport extends PublishRsyncKeys
  import autoImport._

  override def projectSettings = publishRsyncSettings()

  def publishRsyncSettings(): Seq[Setting[_]] = Seq(
    publishRsync := {
      val (from, to) = publishRsyncArtifact.value
      Process(Seq("rsync", "-azP", s"$from/", s"${publishRsyncHost.value}:$to"),
              None,
              "RSYNC_RSH" -> "ssh -o StrictHostKeyChecking=no").! match {
        case 0 => () // success
        case error => throw new IllegalStateException(s"rsync command exited with an error code $error")
      }
    }
  )
}
