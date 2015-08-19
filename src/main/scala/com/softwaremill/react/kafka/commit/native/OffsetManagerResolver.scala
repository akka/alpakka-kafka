package com.softwaremill.react.kafka.commit.native

import kafka.api.{ConsumerMetadataResponse, ConsumerMetadataRequest}
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.consumer.KafkaConsumer
import kafka.network.BlockingChannel

import scala.util.{Failure, Success, Try}

/**
 * Responsible for finding current offset manager in the cluster.
 * NOT THREAD SAFE.
 */
private[native] class OffsetManagerResolver {

  var correlationId = 0

  def resolve(kafkaConsumer: KafkaConsumer[_]): Try[BlockingChannel] = {
    for {
      initialChannel <- connectToAnyBroker(kafkaConsumer.props.brokerList)
      coordinator <- getCoordinator(initialChannel, kafkaConsumer)
      finalChannel <- coordinatorOrInitialChannel(coordinator, initialChannel)
    } yield finalChannel // TODO retry
  }

  private def coordinatorOrInitialChannel(coordinator: Broker, initialChannel: BlockingChannel): Try[BlockingChannel] = {
    if (coordinator.host == initialChannel.host && coordinator.port == initialChannel.port)
      Success(initialChannel)
    else
      connectToChannel(BrokerLocation(coordinator.host, coordinator.port))
  }

  private def connectToAnyBroker(brokerList: String) = {
    val brokers = extractAllBrokers(brokerList)
    val firstBroker = brokers.head // TODO cycle
    connectToChannel(firstBroker)
  }

  private def connectToChannel(location: BrokerLocation): Try[BlockingChannel] = {
    Try {
      val channel = new BlockingChannel(location.host, location.port,
        BlockingChannel.UseDefaultBufferSize,
        BlockingChannel.UseDefaultBufferSize,
        OffsetManagerResolver.ChannelReadTimeoutMs)
      channel.connect()
      channel
    }
  }

  private def extractAllBrokers(brokerList: String): Vector[BrokerLocation] = {
    brokerList.split(',').map {
      elem =>
        val Array(hostStr, portStr) = elem.split(':')
        BrokerLocation(hostStr, portStr.toInt)
    }.toVector
  }

  private def getCoordinator(channel: BlockingChannel, consumer: KafkaConsumer[_]): Try[Broker] = {
    correlationId = correlationId + 1
    val group = consumer.props.groupId
    Try {
      channel.send(new ConsumerMetadataRequest(group, ConsumerMetadataRequest.CurrentVersion, correlationId))
      ConsumerMetadataResponse.readFrom(channel.receive().buffer)
    }.flatMap { metadataResponse =>
      if (metadataResponse.errorCode == ErrorMapping.NoError)
        metadataResponse.coordinatorOpt.map(Success(_)).getOrElse(Failure(new IllegalStateException("Missing coordinator")))
      else
        Failure(new IllegalStateException(s"Cannot connect to coordinator. Error code: ${metadataResponse.errorCode}"))
    }
  }

}

private[native] object OffsetManagerResolver {
  val ChannelReadTimeoutMs = 5000
}

private[native] case class BrokerLocation(host: String, port: Int)