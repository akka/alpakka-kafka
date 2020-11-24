/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.kafka.scaladsl

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorLogging, DeadLetter, Props}
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
import org.slf4j.Logger

import scala.collection.mutable.{Set => MSet}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

class RebalanceExtSpec extends SpecBase with TestcontainersKafkaLike with Inside {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(30.seconds, 500.millis)
  implicit val maxAwait: Duration = 10.minute

  final val consumerClientId1 = "consumer-1"
  final val consumerClientId2 = "consumer-2"

  class DeadLetterListener(log: Logger) extends Actor with ActorLogging {
    override def receive: Receive = {
      case deadLetterMsg: DeadLetter =>
        log.info(
          s"DeadLetterListener::Received dead letter deadLetterMsg: sender >>${deadLetterMsg.sender}<< recipient >>${deadLetterMsg.recipient}<< message >>${deadLetterMsg.message}<<"
        )
    }
  }
  final val deadLetterListener = system.actorOf(Props(new DeadLetterListener(log)))
  system.eventStream.subscribe(deadLetterListener, classOf[DeadLetter])

  class RebalanceListenerActor(topicGroupId: String, logger: Logger) extends Actor with ActorLogging {
    def receive: Receive = {
      case TopicPartitionsAssigned(subscription, topicPartitions) =>
        logger.info(
          s"TopicPartitionsAssigned: self $self sender $sender topicGroupId $topicGroupId topicPartitions $topicPartitions subscription $subscription"
        )
      case TopicPartitionsRevoked(subscription, topicPartitions) =>
        logger.info(
          s"TopicPartitionsRevoked: self $self sender $sender topicGroupId $topicGroupId topicPartitions $topicPartitions subscription $subscription"
        )
    }
  }

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
      .withCloseTimeout(5.seconds)
      .withPollInterval(300.millis)
      .withPollTimeout(200.millis)
      .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords) // 500 is the default value
      .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, partitionAssignmentStrategy)

  def subscribeAndConsumeMessages(
      clientId: String,
      perPartitionMessageCount: Int,
      topicPartitionFutureMap: Map[String, Promise[Done]],
      messageStoreAndAck: Map[Int, (AtomicInteger, Promise[Done], Promise[Done])],
      consumerSettings: ConsumerSettings[String, String],
      subscription: AutoSubscription,
      tpCount: Int,
      ptCount: Int,
      sharedKillSwitch: SharedKillSwitch,
      businessFlow: (String,
                     Int,
                     Map[String, Promise[Done]],
                     Map[Int, (AtomicInteger, Promise[Done], Promise[Done])],
                     SharedKillSwitch,
                     String) => Flow[CommittableMessage[String, String], CommittableOffset, NotUsed]
  ): (Consumer.Control, Future[Seq[Future[Done]]]) = {
    val logPrefix = clientId
    val rebalanceListener = system.actorOf(Props(new RebalanceListenerActor(clientId, log)))
    Consumer
      .committablePartitionedSource(
        consumerSettings.withClientId(clientId),
        subscription
          .withPartitionAssignmentHandler(assignmentHandler(clientId))
          .withRebalanceListener(rebalanceListener)
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
                topicPartitionFutureMap,
                messageStoreAndAck,
                sharedKillSwitch,
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
  ): (Map[String, MSet[TopicPartition]],
      Map[String, Promise[Done]],
      Map[Int, String],
      Set[String],
      Map[Int, String],
      Seq[Future[Done]],
      Map[Int, (AtomicInteger, Promise[Done], Promise[Done])]) = {
    var topicMap = Map[String, MSet[TopicPartition]]()
    var topicPartitionFutureMap = Map[String, Promise[Done]]()
    var topicIdxMap = Map[Int, String]()
    (1 to topicCount).foreach(topicIdx => {
      val topic1 = createTopic(topicIdx, partitions = partitionCount)
      log.debug(s"created topic topic1=$topic1")
      topicIdxMap = topicIdxMap.updated(topicIdx, topic1)
      topicMap = topicMap.updated(topic1, MSet[TopicPartition]())
      (1 to partitionCount).foreach(partitionIdx => {
        val tp1 = new TopicPartition(topic1, partitionIdx - 1)
        topicMap(topic1).add(tp1)
        topicPartitionFutureMap = topicPartitionFutureMap.updated(s"$topic1-${partitionIdx - 1}", Promise[Done])
      })
    })
    val (msgTpMap, producers, messageStoreAndAck) =
      publishMessages(topicCount, partitionCount, perPartitionMessageCount, topicIdxMap)
    (topicMap, topicPartitionFutureMap, topicIdxMap, topicMap.keySet, msgTpMap, producers, messageStoreAndAck)
  }

  // publish a numeric series tagged messages across all topic-partitions
  // e.g., for 18 messages:  messageId(1 to 9) => topic-1-1-0; messageId(10 to 18) => topic-1-1-1
  def publishMessages(
      topicCount: Int,
      partitionCount: Int,
      perPartitionMessageCount: Int,
      topicIdxMap: Map[Int, String]
  ): (Map[Int, String], Seq[Future[Done]], Map[Int, (AtomicInteger, Promise[Done], Promise[Done])]) = {
    var msgTpMap = Map[Int, String]()
    var messageStoreAndAck = Map[Int, (AtomicInteger, Promise[Done], Promise[Done])]()
    val producers: Seq[Future[Done]] = topicIdxMap.keySet
      .map { topicIdx =>
        val topic1 = topicIdxMap(topicIdx)
        val topicOffset = (topicIdx - 1) * partitionCount * perPartitionMessageCount
        (0 until partitionCount).toSet.map { partitionIdx: Int =>
          val startMessageIdx = partitionIdx * perPartitionMessageCount + 1 + topicOffset
          val endMessageIdx = startMessageIdx + perPartitionMessageCount - 1
          val messageRange = startMessageIdx to endMessageIdx
          messageRange.foreach(messageId => {
            val topicPartition = s"$topic1-$partitionIdx"
            msgTpMap = msgTpMap.updated(messageId, topicPartition)
            messageStoreAndAck =
              messageStoreAndAck.updated(messageId, (new AtomicInteger(0), Promise[Done], Promise[Done]))
          })
          produce(topic1, messageRange, partitionIdx).map { f =>
            val topicPartition = s"$topic1-$partitionIdx"
            log.debug(s"Produced messages $startMessageIdx to $endMessageIdx to topicPartition $topicPartition")
            f
          }
        }.toSeq
      }
      .toSeq
      .flatten
    (msgTpMap, producers, messageStoreAndAck)
  }

  def businessFlow1(clientId: String,
                    perPartitionMessageCount: Int,
                    topicPartitionFutureMap: Map[String, Promise[Done]],
                    messageStoreAndAck: Map[Int, (AtomicInteger, Promise[Done], Promise[Done])],
                    sharedKillSwitch: SharedKillSwitch,
                    logPrefix: String): Flow[CommittableMessage[String, String], CommittableOffset, NotUsed] = {
    Flow.fromFunction { message =>
      val messageVal = message.record.value.toInt
      val duplicateCount = messageStoreAndAck(messageVal)._1.incrementAndGet()
      val topicPartition = s"${message.record.topic}-${message.record.partition}"
      val msg1 =
        s"$logPrefix::businessFlow1:offset=${message.committableOffset.partitionOffset.offset} messageId=$messageVal topicPartition=${message.record.topic}-${message.record.partition} consumerId=$clientId duplicateCount=$duplicateCount"
      if (duplicateCount > 1) {
        log.warn(s"businessFlow1:received:$msg1")
      } else {
        log.info(s"businessFlow1:received:$msg1")
      }
      // by default threads are blocked here, waiting for a manual unblock as per testing steps
      log.info(s"$logPrefix::businessFlow1::BEGIN:blockAtMessage=$messageVal:: $msg1")
      Await.result(messageStoreAndAck(messageVal)._2.future, maxAwait)
      val promise1 = messageStoreAndAck(messageVal)._3
      if (!promise1.isCompleted) {
        promise1.complete(Try(Done))
        log.info(s"$logPrefix::businessFlow1:DONE:blockAtMessage=$messageVal:: $msg1")
      } else {
        log.info(s"$logPrefix::businessFlow1:IGNORE:blockAtMessage=$messageVal:: $msg1")
      }
      log.info(s"$logPrefix::businessFlow1::END:blockAtMessage=$messageVal:: $msg1")
      if (message.committableOffset.partitionOffset.offset == perPartitionMessageCount - 1) {
        val promise1 = topicPartitionFutureMap(topicPartition)
        if (!promise1.isCompleted) {
          log.info(s"$logPrefix::businessFlow1::promise:completing")
          promise1.complete(Try(Done))
        } else {
          log.warn(s"$logPrefix::businessFlow1::promise:already completed")
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
      val maxAwait = 10.seconds
      // create topic-partition map and publish messages
      // messageId(1 to 9) => topic-1-1-0
      // messageId(10 to 18) => topic-1-1-1
      val (topicMap, topicPartitionFutureMap, topicIdxMap, topicSet, msgTpMap, producers, messageStoreAndAck) =
        createTopicMapsAndPublishMessages(topicCount, partitionCount, perPartitionMessageCount)
      val group1 = createGroupId(1)
      val consumerSettings1 = consumerSettings(group1, "3", classOf[AlpakkaAssignor].getName)

      // let producers publish all messages
      Await.result(Future.sequence(producers), maxAwait)

      val topic1PartitionList = topicMap.getOrElse(topicIdxMap.getOrElse(1, null), null).toList
      // because of set sorting tlp0 is at idx 1 and tlp11 is at index 0
      val t1p0 = topic1PartitionList(1)
      val t1p1 = topic1PartitionList(0)
      AlpakkaAssignor.clientIdToPartitionMap.set(
        Map(
          consumerClientId1 -> Set(t1p0),
          consumerClientId2 -> Set(t1p1)
        )
      )

      // consumer-1::introduce first consumer with topic-1-1-0 assigned to its SubSource-topic-1-1-0-A
      val subscription = Subscriptions.topics(topicSet)
      val sharedKillSwitch1: SharedKillSwitch = KillSwitches.shared(consumerClientId1)
      val (control1, _) =
        subscribeAndConsumeMessages(
          consumerClientId1,
          perPartitionMessageCount,
          topicPartitionFutureMap,
          messageStoreAndAck,
          consumerSettings1,
          subscription,
          topicCount,
          partitionCount,
          sharedKillSwitch1,
          businessFlow1
        )
      // consumer-1::SubSource-topic-1-1-0-A:confirm first messadeId=1 is received and committed from message batch (1,2,3)
      messageStoreAndAck(1)._2.complete(Try(Done))
      Await.result(messageStoreAndAck(1)._3.future, maxAwait)
      // consumer-1::SubSource-topic-1-1-0-A:verify messageId=1 is received in the business logic function
      assert(messageStoreAndAck(1)._1.intValue() == 1)

      // consumer-2::introduce second consumer with stopic-1-1-1 assigned to its SubSource-1-1-1-A
      val sharedKillSwitch2: SharedKillSwitch = KillSwitches.shared(consumerClientId2)
      val (control2, _) =
        subscribeAndConsumeMessages(
          consumerClientId2,
          perPartitionMessageCount,
          topicPartitionFutureMap,
          messageStoreAndAck,
          consumerSettings1,
          subscription,
          topicCount,
          partitionCount,
          sharedKillSwitch2,
          businessFlow1
        )
      // consumer-2::SubSource-topic-1-1-1-A:confirm first messageId=10 is received and committed from batch (10,11,12)
      messageStoreAndAck(10)._2.complete(Try(Done))
      Await.result(messageStoreAndAck(10)._3.future, maxAwait)
      // consumer-2::SubSource-topic-1-1-1-A:verify messageId=10 is received in the business logic function
      assert(messageStoreAndAck(10)._1.intValue() == 1)

      // consumer-2::define post-abort partition distribution
      AlpakkaAssignor.clientIdToPartitionMap.set(
        Map(
          consumerClientId2 -> Set(t1p0, t1p1)
        )
      )
      // consumer-1::SubSource-topic-1-1-0-A:unblock messageId=2 from batch (1,2,3)
      messageStoreAndAck(2)._2.complete(Try(Done))
      // consumer-1::SubSource-topic-1-1-0-A:verify messageId=2 thread is unblocked
      Await.result(messageStoreAndAck(2)._3.future, maxAwait)
      // assert(messageStoreAndAck(2)._1.intValue() == 1)

      // consumer-1::SubSource-topic-1-1-0-A:abort at messageId=2 (uncommitted)
      sharedKillSwitch1.abort(new Throwable(s"abort $consumerClientId1 messageId=2"))

      // consumer-2::after abort two new sub sources serve topic-1-1-0 and topic-1-1-1: SubSource-topic-1-1-0-B and SubSource-topic-1-1-1-B

      // consumer-2::SubSource-topic-1-1-1-A:unblock second message from batch (10,11,12)
      messageStoreAndAck(11)._2.complete(Try(Done))
      Await.result(messageStoreAndAck(11)._3.future, maxAwait)
      // assert(messageStoreAndAck(11)._1.intValue() == 1)

      // consumer-2::SubSource-topic-1-1-0-B starts consuming at its first batch (2,3,4)
      // consumer-2::SubSource-topic-1-1-1-B starts consuming at its first batch (12,13,14)

      // consumer-1::SubSource-topic-1-1-0-A:unblock last message from batch (1,2,3)
      // consumer-2::SubSource-topic-1-1-0-B:unblock second message from batch (2,3,4)
      messageStoreAndAck(3)._2.complete(Try(Done))
      Await.result(messageStoreAndAck(3)._3.future, maxAwait)

      // consumer-1::SubSource-topic-1-1-0-A:Terminates

      // consumer-2::SubSource-topic-1-1-0-B:unblock last message from batch (2,3,4)
      messageStoreAndAck(4)._2.complete(Try(Done))
      Await.result(messageStoreAndAck(4)._3.future, maxAwait)
      // assert(messageStoreAndAck(2)._1.intValue() == 2) // message replay
      // assert(messageStoreAndAck(3)._1.intValue() == 2) // message replay
      // assert(messageStoreAndAck(4)._1.intValue() == 1)

      // consumer-2::SubSource-topic-1-1-0-B:issues RequestMessage for the next batch

      // consumer-2::SubSource-topic-1-1-0-B:unblock first message from batch (5,6,7)
      messageStoreAndAck(5)._2.complete(Try(Done))
      Await.result(messageStoreAndAck(5)._3.future, maxAwait)
      // assert(messageStoreAndAck(5)._1.intValue() == 1)

      // consumer-2::SubSource-topic-1-1-1-A:unblock last message from batch (10,11,12)
      // consumer-2::SubSource-topic-1-1-1-A:issues **problematic** RequestMessage requesting the next batch
      // consumer-2::SubSource-topic-1-1-1-B:unblock first message from batch (12,13,14)
      messageStoreAndAck(12)._2.complete(Try(Done))
      Await.result(messageStoreAndAck(12)._3.future, maxAwait)
      // assert(messageStoreAndAck(12)._1.intValue() == 2) // message replay

      // consumer-2::KafkaConsumerActor receives message batch (15,16,17) and forwards it to the **defunct** SubSource-topic-1-1-1-A
      // consumer-2::SubSource-topic-1-1-1-A:Terminates
      // consumer-2::Above message sequence is essential for the lost message batch (15,16,17)
      // consumer-2::DeadLetterListener receives batch (15,16,17) destine for SubSource-topic-1-1-1-A

      // consumer-2::SubSource-topic-1-1-1-B:unblock second message from batch (12,13,14)
      messageStoreAndAck(13)._2.complete(Try(Done))
      Await.result(messageStoreAndAck(13)._3.future, maxAwait)
      // assert(messageStoreAndAck(13)._1.intValue() == 1)

      // consumer-2::SubSource-topic-1-1-0-B:unblock second message from batch (5,6,7)
      // consumer-2::SubSource-topic-1-1-1-B:issues RequestMessage requesting the next batch
      messageStoreAndAck(6)._2.complete(Try(Done))
      Await.result(messageStoreAndAck(6)._3.future, maxAwait)
      // assert(messageStoreAndAck(6)._1.intValue() == 1)

      // consumer-2::SubSource-topic-1-1-1-B:unblock last message from batch (12,13,14)
      // consumer-2::SubSource-topic-1-1-1-B:issues RequestMessage requesting the next batch
      messageStoreAndAck(14)._2.complete(Try(Done))
      Await.result(messageStoreAndAck(14)._3.future, maxAwait)
      // assert(messageStoreAndAck(14)._1.intValue() == 1)

      // consumer-2::KafkaConsumerActor receives message batch (18) and forwards it to the SubSource-topic-1-1-1-B

      // unblock all remaining messages
      val publishedMessageCount = topicCount * partitionCount * perPartitionMessageCount
      (1 to publishedMessageCount).foreach(
        messageId =>
          if (!messageStoreAndAck(messageId)._2.isCompleted) {
            messageStoreAndAck(messageId)._2.complete(Try(Done))
          }
      )
      // wait until last message from each partition is consumed
      Await.result(Future.sequence(topicPartitionFutureMap.values.map(_.future)), maxAwait)

      // shutdown system
      control1.shutdown().futureValue shouldBe Done
      control2.shutdown().futureValue shouldBe Done
      sharedKillSwitch1.shutdown()
      sharedKillSwitch1.shutdown()

      // analyze received messages
      val consumedMessages = messageStoreAndAck.filter(_._2._1.intValue > 0)
      log.debug(
        s"consumedMessages.size=${consumedMessages.size} publishedMessageCount=$publishedMessageCount"
      )
      if (consumedMessages.size != publishedMessageCount) {
        val s1 = 1 to publishedMessageCount
        val s2 = consumedMessages.keySet
        log.error(s"FAILURE::missing messages found ${s1.size} != ${s2.size}")
        s1.filter(!s2.contains(_)).foreach(m => log.error(s"FAILURE::missing message $m topicPartition ${msgTpMap(m)}"))
      }
      consumedMessages.size shouldBe publishedMessageCount

      //fail("uncomment me to dump logs for successful run")
    }

  }

}
