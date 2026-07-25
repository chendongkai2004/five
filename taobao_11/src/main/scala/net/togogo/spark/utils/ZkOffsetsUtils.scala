package net.togogo.spark.utils

import kafka.common.TopicAndPartition
import kafka.utils.{ZKGroupTopicDirs, ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}
import net.togogo.spark.config.ApplicationConfig

object ZkOffsetsUtils {
  def loadFromOffsets(topics: Array[String], groupId: String): Map[TopicAndPartition, Long] = {
    val zkClient = new ZkClient(ApplicationConfig.KAFKA_ZK_URL, 30000, 30000, ZKStringSerializer)
    import scala.collection.mutable
    var fromOffsets = mutable.Map[TopicAndPartition, Long]()
    topics.foreach { topicName =>
      val children = zkClient.countChildren(ZkUtils.getTopicPartitionsPath(topicName))
      (0 until children).foreach { partitionId =>
        val tp = TopicAndPartition(topicName, partitionId)
        val consumerOffsetDir = new ZKGroupTopicDirs(groupId, topicName).consumerOffsetDir
        val path = s"$consumerOffsetDir/$partitionId"
        if (zkClient.exists(path)) {
          val offset = zkClient.readData[String](path).toLong
          fromOffsets += (tp -> offset)
        } else {
          fromOffsets += (tp -> 0L)
        }
      }
    }
    zkClient.close()
    fromOffsets.toMap
  }

  def saveUtilOffsets(offsetRange: OffsetRange, groupId: String): Unit = {
    val tp = TopicAndPartition(offsetRange.topic, offsetRange.partition)
    val kafkaCluster = new KafkaCluster(Map("bootstrap.servers" -> ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS))
    kafkaCluster.setConsumerOffsets(groupId, Map(tp -> offsetRange.untilOffset))
  }
}
