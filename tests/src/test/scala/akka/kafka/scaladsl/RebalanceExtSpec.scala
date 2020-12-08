/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka._
import akka.kafka.testkit.scaladsl.TestcontainersKafkaLike
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.{Done, NotUsed}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

// https://github.com/akka/alpakka-kafka/pull/1263
class RebalanceExtSpec extends SpecBase with TestcontainersKafkaLike with Inside {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(30.seconds, 500.millis)

  final val consumerClientId1 = "consumer-1"
  final val consumerClientId2 = "consumer-2"

  case class TopicPartitionMetaData(topics: List[String],
                                    tps: List[TopicPartition],
                                    tpFutureMap: Map[String, Promise[Done]],
                                    producerTpsAck: Seq[Future[Done]],
                                    messageStoreAndAck: Map[Int, MessageAck])

  case class MessageAck(partitionName: String,
                        messageCounter: AtomicInteger,
                        waitUntil: Promise[Done],
                        ackWaitUntil: Promise[Done])

  def assignmentHandler(clientId: String): PartitionAssignmentHandler = new PartitionAssignmentHandler {
    override def onAssign(tps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
      log.debug(s"assignmentHandler::onAssign: client_id $clientId tps $tps consumer $consumer ")
    }
    override def onRevoke(tps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
      log.debug(s"assignmentHandler::onRevoke: client_id $clientId tps $tps consumer $consumer ")
    }
    override def onLost(tps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
      log.debug(s"assignmentHandler::onLost: client_id $clientId tps $tps consumer $consumer ")
    }
    override def onStop(tps: Set[TopicPartition], consumer: RestrictedConsumer): Unit = {
      log.debug(s"assignmentHandler::onStop: client_id $clientId tps $tps consumer $consumer ")
    }
  }

  def consumerSettings(group: String,
                       maxPollRecords: String,
                       partitionAssignmentStrategy: String): ConsumerSettings[String, String] =
    consumerDefaults
      .withGroupId(group)
      .withPollInterval(400.millis) // default 50.ms, failure is more prominent between (300 to 500).millis
      .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords) // default 500
      .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy)

  def subscribeAndConsumeMessages(clientId: String,
                                  perPartitionMessageCount: Int,
                                  topicMetaData: TopicPartitionMetaData,
                                  consumerSettings: ConsumerSettings[String, String],
                                  subscription: AutoSubscription,
                                  sharedKillSwitch: SharedKillSwitch): (Consumer.Control, Future[Seq[Future[Done]]]) = {
    val logPrefix = clientId
    Consumer
      .committablePartitionedSource(
        consumerSettings.withClientId(clientId),
        subscription
          .withPartitionAssignmentHandler(assignmentHandler(clientId))
      )
      .map {
        case (topicPartition, topicPartitionStream) =>
          log.debug(s"$logPrefix::Consuming partitioned source clientId: $clientId, for tp: $topicPartition")
          val innerStream: Source[ConsumerMessage.CommittableOffsetBatch, NotUsed] = topicPartitionStream
            .via(sharedKillSwitch.flow)
            .via(
              businessFlow(
                clientId,
                perPartitionMessageCount,
                topicMetaData,
                logPrefix
              )
            )
            .via(Committer.batchFlow(committerDefaults.withMaxBatch(1)))
          innerStream.runWith(Sink.ignore)
      }
      .via(sharedKillSwitch.flow)
      .toMat(Sink.seq)(Keep.both)
      .run()
  }

  // create topic-partition map
  def createTopicMapsAndPublishMessages(
      topicCount: Int,
      partitionCount: Int,
      perPartitionMessageCount: Int
  ): TopicPartitionMetaData = {
    var tps = List[TopicPartition]()
    var topics = List[String]()
    var tpFutureMap = Map[String, Promise[Done]]()
    (1 to topicCount).foreach(topicIdx => {
      val topic1 = createTopic(topicIdx, partitions = partitionCount)
      log.debug(s"created topic topic1=$topic1")
      topics = topics :+ topic1
      (0 until partitionCount).foreach(partitionIdx => {
        val tp1 = new TopicPartition(topic1, partitionIdx)
        tps = tps :+ tp1
        tpFutureMap = tpFutureMap.updated(s"$topic1-$partitionIdx", Promise[Done])
      })
    })
    val (producerTpsAck, messageStoreAndAck) = publishMessages(partitionCount, perPartitionMessageCount, topics)
    TopicPartitionMetaData(topics, tps, tpFutureMap, producerTpsAck, messageStoreAndAck)
  }

  // publish a numeric series tagged messages across all topic-partitions
  // e.g., for 18 messages:  messageId(1 to 9) => topic-1-1-0; messageId(10 to 18) => topic-1-1-1
  def publishMessages(
      partitionCount: Int,
      perPartitionMessageCount: Int,
      topics: List[String]
  ): (Seq[Future[Done]], Map[Int, MessageAck]) = {
    var messageStoreAndAck = Map[Int, MessageAck]()
    val producerTpsAck: Seq[Future[Done]] = topics.flatMap { topic1 =>
      val topicIdx = topics.indexOf(topic1)
      val topicOffset = topicIdx * partitionCount * perPartitionMessageCount
      (0 until partitionCount).map { partitionIdx: Int =>
        val startMessageIdx = partitionIdx * perPartitionMessageCount + 1 + topicOffset
        val endMessageIdx = startMessageIdx + perPartitionMessageCount - 1
        val messageRange = startMessageIdx to endMessageIdx
        messageRange.foreach(messageId => {
          val partitionName = s"$topic1-$partitionIdx"
          messageStoreAndAck =
            messageStoreAndAck.updated(messageId,
                                       MessageAck(partitionName, new AtomicInteger(0), Promise[Done], Promise[Done]))
        })
        produce(topic1, messageRange, partitionIdx).map { f =>
          val partitionName = s"$topic1-$partitionIdx"
          log.debug(
            s"publishMessages::published messages from ($startMessageIdx to $endMessageIdx) to partitionName $partitionName"
          )
          f
        }
      }
    }
    (producerTpsAck, messageStoreAndAck)
  }

  def businessFlow(clientId: String,
                   perPartitionMessageCount: Int,
                   topicMetadata: TopicPartitionMetaData,
                   logPrefix: String): Flow[CommittableMessage[String, String], CommittableOffset, NotUsed] = {
    Flow.fromFunction { message =>
      val messageVal = message.record.value.toInt
      val duplicateCount = topicMetadata.messageStoreAndAck(messageVal).messageCounter.incrementAndGet()
      val partitionName = s"${message.record.topic}-${message.record.partition}"
      val msg1 =
        s"$logPrefix::businessFlow:offset=${message.committableOffset.partitionOffset.offset} messageId=$messageVal partitionName=$partitionName consumerId=$clientId duplicateCount=$duplicateCount"
      if (duplicateCount > 1) {
        log.warn(s"businessFlow:duplicate:$msg1")
      } else {
        log.info(s"businessFlow:received:$msg1")
      }
      // by default threads are blocked here, waiting for a manual unblock as per testing steps
      log.info(s"$logPrefix::businessFlow::begin:blockAtMessage=$messageVal:: $msg1")
      Await.result(topicMetadata.messageStoreAndAck(messageVal).waitUntil.future, remainingOrDefault)
      val ackWaitUntilPromise = topicMetadata.messageStoreAndAck(messageVal).ackWaitUntil
      if (ackWaitUntilPromise.isCompleted) {
        log.info(s"$logPrefix::businessFlow:ignore:blockAtMessage=$messageVal:: $msg1")
      } else {
        ackWaitUntilPromise.complete(Try(Done))
        log.info(s"$logPrefix::businessFlow:end:blockAtMessage=$messageVal:: $msg1")
      }
      if (message.committableOffset.partitionOffset.offset == perPartitionMessageCount - 1) {
        val lastMessagePromise = topicMetadata.tpFutureMap(partitionName)
        if (lastMessagePromise.isCompleted) {
          log.warn(s"$logPrefix::businessFlow::promise:already completed:: $msg1")
        } else {
          log.info(s"$logPrefix::businessFlow::promise:completing:: $msg1")
          lastMessagePromise.complete(Try(Done))
        }
      }
      message.committableOffset
    }
  }

  "Fetched records" must {

    "no messages should be lost when two consumers consume from one topic and two partitions and one consumer aborts mid-stream" in assertAllStagesStopped {
      val topicCount = 1
      val partitionCount = 2
      val perPartitionMessageCount = 9
      // create topic-partition map and publish messages
      // messageId(1 to 9) => topic-1-1-0
      // messageId(10 to 18) => topic-1-1-1
      val topicMetadata: TopicPartitionMetaData =
        createTopicMapsAndPublishMessages(topicCount, partitionCount, perPartitionMessageCount)
      val group1 = createGroupId(1)
      val consumerSettings1 = consumerSettings(group1, "3", classOf[AlpakkaAssignor].getName)

      // let producers publish all messages
      Await.result(Future.sequence(topicMetadata.producerTpsAck), remainingOrDefault)

      val topic1PartitionList =
        topicMetadata.tps.sortBy(a => (a.topic, a.partition))
      val t1p0 = topic1PartitionList(0)
      val t1p1 = topic1PartitionList(1)
      AlpakkaAssignor.clientIdToPartitionMap.set(
        Map(
          consumerClientId1 -> Set(t1p0),
          consumerClientId2 -> Set(t1p1)
        )
      )

      // consumer-1::introduce first consumer with topic-1-1-0 assigned to its SubSource-topic-1-1-0-A
      val subscription = Subscriptions.topics(topicMetadata.topics.toSet)
      val sharedKillSwitch1: SharedKillSwitch = KillSwitches.shared(consumerClientId1)
      val (control1, _) =
        subscribeAndConsumeMessages(consumerClientId1,
                                    perPartitionMessageCount,
                                    topicMetadata,
                                    consumerSettings1,
                                    subscription,
                                    sharedKillSwitch1)
      // consumer-1::SubSource-topic-1-1-0-A:confirm first messageId=1 is received and committed from message batch (1,2,3)
      topicMetadata.messageStoreAndAck(1).waitUntil.complete(Try(Done))
      Await.result(topicMetadata.messageStoreAndAck(1).ackWaitUntil.future, remainingOrDefault)
      // consumer-1::SubSource-topic-1-1-0-A:verify messageId=1 is received in the business logic function
      assert(topicMetadata.messageStoreAndAck(1).messageCounter.intValue() == 1)

      // consumer-2::introduce second consumer with topic-1-1-1 assigned to its SubSource-1-1-1-A
      val sharedKillSwitch2: SharedKillSwitch = KillSwitches.shared(consumerClientId2)
      val (control2, _) =
        subscribeAndConsumeMessages(consumerClientId2,
                                    perPartitionMessageCount,
                                    topicMetadata,
                                    consumerSettings1,
                                    subscription,
                                    sharedKillSwitch2)
      // consumer-2::SubSource-topic-1-1-1-A:confirm first messageId=10 is received and committed from batch (10,11,12)
      topicMetadata.messageStoreAndAck(10).waitUntil.complete(Try(Done))
      Await.result(topicMetadata.messageStoreAndAck(10).ackWaitUntil.future, remainingOrDefault)
      // consumer-2::SubSource-topic-1-1-1-A:verify messageId=10 is received in the business logic function

      // consumer-2::define post-abort partition distribution
      AlpakkaAssignor.clientIdToPartitionMap.set(
        Map(
          consumerClientId2 -> Set(t1p0, t1p1)
        )
      )
      // consumer-1::SubSource-topic-1-1-0-A:unblock messageId=2 from batch (1,2,3)
      topicMetadata.messageStoreAndAck(2).waitUntil.complete(Try(Done))
      // consumer-1::SubSource-topic-1-1-0-A:verify messageId=2 thread is unblocked
      Await.result(topicMetadata.messageStoreAndAck(2).ackWaitUntil.future, remainingOrDefault)

      // consumer-1::SubSource-topic-1-1-0-A:abort at messageId=2 (uncommitted)
      sharedKillSwitch1.abort(new Throwable(s"abort $consumerClientId1 messageId=2"))

      // consumer-2::after abort two new sub sources serve topic-1-1-0 and topic-1-1-1: SubSource-topic-1-1-0-B and SubSource-topic-1-1-1-B

      // consumer-2::SubSource-topic-1-1-1-A:unblock second message from batch (10,11,12)
      topicMetadata.messageStoreAndAck(11).waitUntil.complete(Try(Done))
      Await.result(topicMetadata.messageStoreAndAck(11).ackWaitUntil.future, remainingOrDefault)

      // consumer-2::SubSource-topic-1-1-0-B starts consuming at its first batch (2,3,4)
      // consumer-2::SubSource-topic-1-1-1-B starts consuming at its first batch (12,13,14)

      // consumer-1::SubSource-topic-1-1-0-A:unblock last message from batch (1,2,3)
      // consumer-2::SubSource-topic-1-1-0-B:unblock second message from batch (2,3,4)
      topicMetadata.messageStoreAndAck(3).waitUntil.complete(Try(Done))
      Await.result(topicMetadata.messageStoreAndAck(3).ackWaitUntil.future, remainingOrDefault)

      // consumer-1::SubSource-topic-1-1-0-A:Terminates

      // consumer-2::SubSource-topic-1-1-0-B:unblock last message from batch (2,3,4), messages (2,3) are re-played
      topicMetadata.messageStoreAndAck(4).waitUntil.complete(Try(Done))
      Await.result(topicMetadata.messageStoreAndAck(4).ackWaitUntil.future, remainingOrDefault)

      // consumer-2::SubSource-topic-1-1-0-B:issues RequestMessage for the next batch

      // consumer-2::SubSource-topic-1-1-0-B:unblock first message from batch (5,6,7)
      topicMetadata.messageStoreAndAck(5).waitUntil.complete(Try(Done))
      Await.result(topicMetadata.messageStoreAndAck(5).ackWaitUntil.future, remainingOrDefault)

      // consumer-2::SubSource-topic-1-1-1-A:unblock last message from batch (10,11,12)
      // consumer-2::SubSource-topic-1-1-1-A:issues **problematic** RequestMessage requesting the next batch
      // consumer-2::SubSource-topic-1-1-1-B:unblock first message from batch (12,13,14), message 12 is re-played
      topicMetadata.messageStoreAndAck(12).waitUntil.complete(Try(Done))
      Await.result(topicMetadata.messageStoreAndAck(12).ackWaitUntil.future, remainingOrDefault)

      // consumer-2::KafkaConsumerActor receives message batch (15,16,17) and forwards it to the **defunct** SubSource-topic-1-1-1-A
      // consumer-2::SubSource-topic-1-1-1-A:Terminates
      // consumer-2::Above message sequence is essential for the lost message batch (15,16,17)
      // consumer-2::DeadLetterListener receives batch (15,16,17) destined for SubSource-topic-1-1-1-A

      // consumer-2::SubSource-topic-1-1-1-B:unblock second message from batch (12,13,14)
      topicMetadata.messageStoreAndAck(13).waitUntil.complete(Try(Done))
      Await.result(topicMetadata.messageStoreAndAck(13).ackWaitUntil.future, remainingOrDefault)

      // consumer-2::SubSource-topic-1-1-0-B:unblock second message from batch (5,6,7)
      // consumer-2::SubSource-topic-1-1-1-B:issues RequestMessage requesting the next batch
      topicMetadata.messageStoreAndAck(6).waitUntil.complete(Try(Done))
      Await.result(topicMetadata.messageStoreAndAck(6).ackWaitUntil.future, remainingOrDefault)

      // consumer-2::SubSource-topic-1-1-1-B:unblock last message from batch (12,13,14)
      // consumer-2::SubSource-topic-1-1-1-B:issues RequestMessage requesting the next batch
      topicMetadata.messageStoreAndAck(14).waitUntil.complete(Try(Done))
      Await.result(topicMetadata.messageStoreAndAck(14).ackWaitUntil.future, remainingOrDefault)

      // consumer-2::KafkaConsumerActor receives message batch (18) and forwards it to the SubSource-topic-1-1-1-B

      // unblock all remaining messages
      val publishedMessageCount = topicCount * partitionCount * perPartitionMessageCount
      (1 to publishedMessageCount).foreach(
        messageId =>
          if (!topicMetadata.messageStoreAndAck(messageId).waitUntil.isCompleted) {
            topicMetadata.messageStoreAndAck(messageId).waitUntil.complete(Try(Done))
          }
      )
      // wait until last message from each partition is consumed
      Await.result(Future.sequence(topicMetadata.tpFutureMap.values.map(_.future)), remainingOrDefault)

      // shutdown system
      control1.shutdown().futureValue shouldBe Done
      control2.shutdown().futureValue shouldBe Done
      sharedKillSwitch1.shutdown()
      sharedKillSwitch1.shutdown()

      // analyze received messages
      val consumedMessages = topicMetadata.messageStoreAndAck.filter(_._2.messageCounter.intValue > 0)
      log.debug(
        s"consumedMessages.size=${consumedMessages.size} publishedMessageCount=$publishedMessageCount"
      )
      if (consumedMessages.size != publishedMessageCount) {
        val s1 = 1 to publishedMessageCount
        val s2 = consumedMessages.keySet
        log.error(s"FAILURE::missing messages found ${s1.size} != ${s2.size}")
        s1.filter(!s2.contains(_))
          .foreach(
            m =>
              log.error(
                s"FAILURE::missing message $m topicPartition ${topicMetadata.messageStoreAndAck(m).partitionName}"
              )
          )
      }
      consumedMessages.size shouldBe publishedMessageCount
    }

  }

}
