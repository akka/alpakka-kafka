package com.softwaremill.react.kafka.commit.zk

import com.softwaremill.react.kafka.commit._
import kafka.consumer.KafkaConsumer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.util.{Failure, Success, Try}

class ZkCommitterFactory extends CommitterFactory {

  override def create[T](kafkaConsumer: KafkaConsumer[T]): Either[CommitterCreationError, OffsetCommitter] = {
    val group = kafkaConsumer.props.groupId
    val zkConnect = kafkaConsumer.props.zookeeperConnect

    Try(CuratorFrameworkFactory.newClient(zkConnect, new ExponentialBackoffRetry(256, 1024))) match {
      case Success(curator) => Right(new ZookeeperOffsetCommitter(group, curator))
      case Failure(ex) => Left(StorageConnectionFailed(ex))
    }
  }
}
