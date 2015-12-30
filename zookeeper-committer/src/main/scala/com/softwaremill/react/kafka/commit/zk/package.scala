package com.softwaremill.react.kafka.commit

package object zk {
  import org.apache.curator.utils.ZKPaths

  private[zk] def getPartitionPath(group: String, topic: String, part: Int) =
    ZKPaths.makePath(s"/consumers/$group/offsets/$topic", part.toString)
}
