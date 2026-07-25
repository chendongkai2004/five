package net.togogo.spark.store.hbase

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import net.togogo.spark.config.ApplicationConfig
import net.togogo.spark.utils.{HBaseDao, SparkUtils, StreamingUtils}

object RealTimeOrder2HBase {
  def main(args: Array[String]): Unit = {
    val ssc = SparkUtils.createStreamingContext(this.getClass, 5)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> ApplicationConfig.STREAMING_ETL_GROUP_ID,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(ApplicationConfig.KAFKA_ETL_TOPIC)
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.foreachPartition { iter =>
          val messages = iter.map(_.value())
          HBaseDao.insert(
            ApplicationConfig.HBASE_ORDER_TABLE,
            ApplicationConfig.HBASE_ORDER_TABLE_FAMILY,
            ApplicationConfig.HBASE_ORDER_TABLE_COLUMNS,
            messages
          )
        }
      }
    }

    ssc.start()
    StreamingUtils.stopStreaming(ssc, ApplicationConfig.STOP_HBASE_FILE)
    ssc.awaitTermination()
  }
}
