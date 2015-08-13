package com.softwaremill.react.kafka.commit

import com.cj.kafka.rx._
import kafka.consumer.KafkaConsumer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.util.{Failure, Success, Try}

sealed trait CommiterCreationError
case object StorageNotSupported extends CommiterCreationError
case class ZkConnectionFailed(reason: Throwable) extends CommiterCreationError

class CommitterFactory {

  def create[T](kafkaConsumer: KafkaConsumer[T]): Either[CommiterCreationError, OffsetCommitter] = {
    if (kafkaConsumer.kafkaOffsetStorage)
      Left(StorageNotSupported)
    else {
      val group = kafkaConsumer.props.groupId
      val zkConnect = kafkaConsumer.props.zookeeperConnect

      Try(CuratorFrameworkFactory.newClient(zkConnect, new ExponentialBackoffRetry(256, 1024))) match {
        case Success(curator) => Right(new ZookeeperOffsetCommitter(group, curator))
        case Failure(ex) => Left(ZkConnectionFailed(ex))
      }
    }
  }
}
