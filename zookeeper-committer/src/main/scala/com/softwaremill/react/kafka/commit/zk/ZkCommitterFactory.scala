package com.softwaremill.react.kafka.commit.zk

import com.softwaremill.react.kafka.commit._
import kafka.consumer.KafkaConsumer
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.util.Try

class ZkCommitterFactory extends CommitterFactory {

  override def create(kafkaConsumer: KafkaConsumer[_]): Try[OffsetCommitter] = {
    val group = kafkaConsumer.props.groupId
    val zkConnect = kafkaConsumer.props.zookeeperConnect
    Try(CuratorFrameworkFactory.newClient(zkConnect, new ExponentialBackoffRetry(256, 1024)))
      .map(new ZookeeperOffsetCommitter(group, _))
  }
}
