package net.togogo.spark.report

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataTypes, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import net.togogo.spark.config.ApplicationConfig
import net.togogo.spark.utils.{RedisHelper, SparkUtils, StreamingUtils}

object DailyRealTimeOrderReport {
  def reportAmtTotal(streamDF: DataFrame): Unit = {
    import streamDF.sparkSession.implicits._
    val result = streamDF
      .withWatermark("order_timestamp", "10 minutes")
      .groupBy($"order_date")
      .agg(sum($"money").as("total_amt"))
      .withColumn("prefix", lit("orders:money:total"))
      .withColumn("field", lit("global"))
      .select(
        concat_ws(":", $"prefix", $"order_date").as("key"),
        $"field",
        $"total_amt".cast(StringType).as("value")
      )
    result.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-total")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_TOTAL_CKPT)
      .foreachBatch { (batchDF: DataFrame, _: Long) => RedisHelper.saveToRedis(batchDF) }
      .start()
  }

  def reportAmtProvince(streamDF: DataFrame): Unit = {
    import streamDF.sparkSession.implicits._
    val result = streamDF
      .withWatermark("order_timestamp", "10 minutes")
      .groupBy($"order_date", $"province")
      .agg(sum($"money").as("total_amt"))
      .withColumn("prefix", lit("orders:money:province"))
      .select(
        concat_ws(":", $"prefix", $"order_date").as("key"),
        $"province".as("field"),
        $"total_amt".cast(StringType).as("value")
      )
    result.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-province")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_PROVINCE_CKPT)
      .foreachBatch { (batchDF: DataFrame, _: Long) => RedisHelper.saveToRedis(batchDF) }
      .start()
  }

  def reportAmtCity(streamDF: DataFrame): Unit = {
    val session = streamDF.sparkSession
    import session.implicits._
    val cities = Array("北京市","上海市","深圳市","广州市","杭州市","成都市","南京市","武汉市","西安市")
    val broadcast = session.sparkContext.broadcast(cities)
    val cityFilter = udf((city: String) => broadcast.value.contains(city))
    val result = streamDF
      .filter(cityFilter($"city"))
      .withWatermark("order_timestamp", "10 minutes")
      .groupBy($"order_date", $"city")
      .agg(sum($"money").as("total_amt"))
      .withColumn("prefix", lit("orders:money:city"))
      .select(
        concat_ws(":", $"prefix", $"order_date").as("key"),
        $"city".as("field"),
        $"total_amt".cast(StringType).as("value")
      )
    result.writeStream
      .outputMode(OutputMode.Update())
      .queryName("query-amt-city")
      .option("checkpointLocation", ApplicationConfig.STREAMING_AMT_CITY_CKPT)
      .foreachBatch { (batchDF: DataFrame, _: Long) => RedisHelper.saveToRedis(batchDF) }
      .start()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_ETL_TOPIC)
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()
    val orderDF = kafkaDF.selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter(_.nonEmpty)
      .select(
        to_date(get_json_object($"value", "$.orderTime")).as("order_date"),
        to_timestamp(get_json_object($"value", "$.orderTime")).as("order_timestamp"),
        get_json_object($"value", "$.orderMoney").cast(DataTypes.createDecimalType(10,2)).as("money"),
        get_json_object($"value", "$.province").as("province"),
        get_json_object($"value", "$.city").as("city")
      )
    reportAmtTotal(orderDF)
    reportAmtProvince(orderDF)
    reportAmtCity(orderDF)
    spark.streams.active.foreach { query =>
      StreamingUtils.stopStructuredStreaming(query, ApplicationConfig.STOP_STATE_FILE)
    }
    spark.streams.awaitAnyTermination()
  }
}
