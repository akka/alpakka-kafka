package com.softwaremill.react.kafka.commit.native

import com.softwaremill.react.kafka.commit._
import kafka.api.{ConsumerMetadataRequest, ConsumerMetadataResponse}
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.consumer.KafkaConsumer
import kafka.network.BlockingChannel

import scala.util.{Failure, Success, Try}

class NativeCommitterFactory extends CommitterFactory {

  override def create(kafkaConsumer: KafkaConsumer[_]) = {
    for {
      initialChannel <- connectToAnyBroker(kafkaConsumer.props.brokerList)
      coordinator <- getCoordinator(initialChannel, kafkaConsumer)
      finalChannel <- coordinatorOrInitialChannel(coordinator, initialChannel)
    } yield new NativeCommitter(kafkaConsumer, finalChannel) // TODO retry on error
  }

  def coordinatorOrInitialChannel(coordinator: Broker, initialChannel: BlockingChannel): Try[BlockingChannel] = {
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
        NativeCommitterFactory.ChannelReadTimeoutMs)
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

  def getCoordinator(channel: BlockingChannel, consumer: KafkaConsumer[_]): Try[Broker] = {
    val correlationId = 0 // TODO ??????
    val group = consumer.props.groupId
    val clientId = "clientId" // TODO what is that?
    Try {
      channel.send(new ConsumerMetadataRequest(group, ConsumerMetadataRequest.CurrentVersion, correlationId, clientId))
      ConsumerMetadataResponse.readFrom(channel.receive().buffer)
    }.flatMap { metadataResponse =>
      if (metadataResponse.errorCode == ErrorMapping.NoError)
        metadataResponse.coordinatorOpt.map(Success(_)).getOrElse(Failure(new IllegalStateException("Missing coordinator")))
      else
        Failure(new IllegalStateException(s"Cannot connect to coordinator. Error code: ${metadataResponse.errorCode}"))
    }
  }

}

private[native] object NativeCommitterFactory {
  val ChannelReadTimeoutMs = 5000
}

private[native] case class BrokerLocation(host: String, port: Int)