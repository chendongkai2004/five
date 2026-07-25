package net.togogo.spark.store.es

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.functions._
import net.togogo.spark.config.ApplicationConfig
import net.togogo.spark.utils.{SparkUtils, StreamingUtils}

object RealTimeOrder2Es {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._
    val orderSchema = new StructType()
      .add("orderId", StringType)
      .add("userId", StringType)
      .add("orderTime", StringType)
      .add("ip", StringType)
      .add("orderMoney", StringType)
      .add("orderStatus", StringType)
      .add("province", StringType)
      .add("city", StringType)
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()
    val orderDF = kafkaDF.selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter(_.nonEmpty)
      .select(from_json($"value", orderSchema).as("data"))
      .select($"data.*")
    val query = orderDF.writeStream
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", ApplicationConfig.STREAMING_ES_CKPT)
      .format("es")
      .option("es.nodes", ApplicationConfig.ES_NODES)
      .option("es.port", ApplicationConfig.ES_PORT)
      .option("es.index.auto.create", ApplicationConfig.ES_INDEX_AUTO_CREATE)
      .option("es.write.operation", ApplicationConfig.ES_WRITE_OPERATION)
      .option("es.mapping.id", ApplicationConfig.ES_MAPPING_ID)
      .start(ApplicationConfig.ES_INDEX_NAME)
    StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_ES_FILE)
    query.awaitTermination()
  }
}
