package akka.kafka.scaladsl

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.{Arrays, UUID}

import akka.Done
import akka.cluster.sharding.ShardRegion.ShardId
import akka.kafka._
import akka.kafka.scaladsl.Consumer.DrainingControl
import akka.kafka.testkit.scaladsl.EmbeddedKafkaLike
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.testkit.TestProbe
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.internals.PartitionAssignor
import org.apache.kafka.clients.consumer.{ConsumerConfig, RangeAssignor}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.{Cluster, TopicPartition}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.{Success, Try}

object GroupAssignorSpec {
  val TopicName = "sharding-test-topic"
  val ShardId1 = "shard-id-1"
  val ShardId2 = "shard-id-2"
  val ShardToPartitions = Map(
    ShardId1 -> Set(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1)
    ),
    ShardId2 -> Set(
      new TopicPartition(TopicName, 2),
      new TopicPartition(TopicName, 3)
    )
  )

  val shardIdCounter = new AtomicInteger(1)
  def getShardIdAndIncrement = shardIdCounter.getAndIncrement()
}

class GroupAssignorSpec extends SpecBase(kafkaPort = 9092) with EmbeddedKafkaLike {
  implicit val patience = PatienceConfig(30.seconds, 500.millis)
  import GroupAssignorSpec._

  "GroupAssignor" must {
    "run" in {
      assertAllStagesStopped {
        val partitions = 4
        val totalMessages = 200L

        val topic = createTopic(TopicName, partitions, 1, Map[String,String]().asJava)

        val group = createGroupId(1)
        val consumerSettings = consumerDefaults
          .withGroupId(group)
          .withProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, classOf[AlpakkaAssignor].getName)

        val receivedCounter = new AtomicLong(0L)

        val topicSubscription = Subscriptions.topics(topic)

        def createAndRunConsumer[K,V](settings: ConsumerSettings[K,V], subscription: Subscription) =
          Consumer
            .plainSource(settings, subscription)
            .map { el =>
              receivedCounter.incrementAndGet()
              Done
            }
            .scan(0L)((c, _) => c + 1)
            .toMat(Sink.last)(Keep.both)
            .mapMaterializedValue(DrainingControl.apply)
            .run()

        def createAndRunProducer(elements: immutable.Iterable[Long]) =
          Source(elements)
            .map(n => new ProducerRecord(topic, (n % partitions).toInt, DefaultKey, n.toString))
            .runWith(Producer.plainSink(producerDefaults.withProducer(testProducer)))

        val control = createAndRunConsumer(consumerSettings.withClientId("alpakka-consumer-1"), topicSubscription)
        val control2 = createAndRunConsumer(consumerSettings.withClientId("alpakka-consumer-2"), topicSubscription)

        // waits until partitions are assigned across both consumers
        waitUntilConsumerSummary(group) {
          case consumer1 :: consumer2 :: Nil =>
            val half = partitions / 2
            consumer1.assignment.topicPartitions.size == half && consumer2.assignment.topicPartitions.size == half
        }

        createAndRunProducer(0L until totalMessages / 2).futureValue

        sleep(4.seconds,
          "to get the second consumer started, otherwise it might miss the first messages because of `latest` offset")
        createAndRunProducer(totalMessages / 2 until totalMessages).futureValue

        if (receivedCounter.get() != totalMessages)
          log.warn("All consumers together did receive {}, not the total of {} messages",
            receivedCounter.get(),
            totalMessages)

        val stream1messages = control.drainAndShutdown().futureValue
        val stream2messages = control2.drainAndShutdown().futureValue
        if (stream1messages + stream2messages != totalMessages)
          log.warn(
            "The consumers counted {} + {} = {} messages, not the total of {} messages",
            // boxing for Scala 2.11
            Long.box(stream1messages),
            Long.box(stream2messages),
            Long.box(stream1messages + stream2messages),
            Long.box(totalMessages)
          )
      }
    }
  }

  def createTopic(topicName: String, partitions: Int, replication: Int, config: java.util.Map[String, String]): String = {
    val createResult = adminClient.createTopics(
      Arrays.asList(new NewTopic(topicName, partitions, replication.toShort).configs(config))
    )
    createResult.all().get(10, TimeUnit.SECONDS)
    topicName
  }
}

class AlpakkaAssignor extends RangeAssignor {
  val DefaultShardId: ShardId = "shard-not-resolved-yet"

  import GroupAssignorSpec._

  val log: Logger = LoggerFactory.getLogger(getClass)

  val id: String = UUID.randomUUID().toString.take(5)
  var isLeader: Boolean = false
  val shardId: Promise[ShardId] = Promise()
  val shardToPartitions: Promise[Map[ShardId, Set[TopicPartition]]] = Promise()

  log.debug(s"[$id] Starting..")

  getShardId()
  getShardToPartitionsMap()

  /**
    * Lookup the shard id of the akka cluster member this consumer group member is running on
    *
    * - how do we pass akka cluster sharding info into the Assignor?
    * - can we use akka gRPC or some other kind of IPC to connect to akka cluster?
    * - access a singleton somewhere?
    */
  private def getShardId(): Unit = {
    Future.successful {
      shardId.complete(Try(s"shard-id-${GroupAssignorSpec.getShardIdAndIncrement}"))
    }
  }

  private def getShardToPartitionsMap(): Unit = {
    Future.successful {
      shardToPartitions.complete(Try(GroupAssignorSpec.ShardToPartitions))
    }
  }

  private def demoteFromLeader(): Unit = isLeader = false

  override def name: String = "alpakka"

  /**
    * Pass along the shard ID this consumer is local to
    */
  override def subscription(topics: util.Set[String]): PartitionAssignor.Subscription = {
    val thisShardId = shardId.future.value match {
      case Some(Success(sid)) => sid
      case _ => DefaultShardId
    }

    val userData = ByteBuffer.wrap(thisShardId.getBytes(StandardCharsets.UTF_8))
    val sub = new PartitionAssignor.Subscription(topics.asScala.toList.asJava, userData)

    log.debug(s"[$id] Sending subscription: $sub, with user data: $thisShardId")

    sub
  }

  override def assign(partitionsPerTopic: util.Map[String, Integer],
                      subscriptions: util.Map[String,PartitionAssignor.Subscription]): util.Map[String, util.List[TopicPartition]] = {

    log.debug(s"[$id] assign: partitionsPerTopic: ${partitionsPerTopic.asScala}\nsubscriptions: ${subscriptions.asScala}")

    if (!isLeader) {
      isLeader = true
      log.debug(s"[$id] I am the leader!")
    }

    val shardToPartitionsMap = shardToPartitions.future.value match {
      case Some(Success(map)) => map
      case _ => ShardToPartitions
    }

    val totalPartitions: Set[TopicPartition] = for {
      (topic, numPartitions) <- partitionsPerTopic.asScala.toSet
      partitionNumber <- 0 until numPartitions
    } yield new TopicPartition(topic, partitionNumber)

    val assignments = subscriptions.asScala.map {
      case (consumerId, sub) =>
        val shardId = StandardCharsets.UTF_8.decode(sub.userData()).toString

        // get valid partitions to assign to this consumer
        val shardPartitions = shardToPartitionsMap.get(shardId).toList.flatMap { partitions =>
          partitions.intersect(totalPartitions).toIterator
        }

        consumerId -> shardPartitions.asJava
    }

    assignments.asJava
  }

  // only the group leader has its `assign` called
  override def assign(metadata: Cluster,
                      subscriptions: util.Map[String, PartitionAssignor.Subscription]): util.Map[String, PartitionAssignor.Assignment] = {
    super.assign(metadata, subscriptions)
  }

  override def onAssignment(assignment: PartitionAssignor.Assignment): Unit = {
    log.debug(s"[$id] My assignment: $assignment")
  }
}