import sbt._
import sbt.Keys._

/**
 * Generate version.conf and akka/kafka/Version.scala files based on the version setting.
 *
 * This was adapted from https://github.com/akka/akka/blob/v2.6.8/project/VersionGenerator.scala
 */
object VersionGenerator {

  val settings: Seq[Setting[_]] = inConfig(Compile)(
    Seq(
      resourceGenerators += generateVersion(resourceManaged, _ / "version.conf", """|akka.kafka.version = "%s"
         |"""),
      sourceGenerators += generateVersion(
          sourceManaged,
          _ / "akka" / "kafka" / "Version.scala",
          """|package akka.kafka
         |
         |object Version {
         |  val current: String = "%s"
         |}
         |"""
        )
    )
  )

  def generateVersion(dir: SettingKey[File], locate: File => File, template: String) = Def.task[Seq[File]] {
    val file = locate(dir.value)
    val content = template.stripMargin.format(version.value)
    if (!file.exists || IO.read(file) != content) IO.write(file, content)
    Seq(file)
  }

}
