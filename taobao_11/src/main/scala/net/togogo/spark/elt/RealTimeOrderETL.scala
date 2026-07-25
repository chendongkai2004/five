package net.togogo.spark.elt

import net.togogo.spark.config.ApplicationConfig
import net.togogo.spark.utils.SparkUtils
import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{get_json_object, struct, to_json, udf}
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

object RealTimeOrderETL extends Logging {

  /**
   * 对流式数据StreamDataFrame进行ETL过滤清洗转换操作
   */
  def streamingProcess(streamDF: DataFrame): DataFrame = {
    val session = streamDF.sparkSession
    import session.implicits._

    // TODO: 对数据进行ETL操作，获取订单状态为0(打开)及转换IP地址为省份和城市
    // 1. 获取订单记录Order Record数据
    val recordStreamDS: Dataset[String] = streamDF
      // 获取value字段的值，转换为String类型
      .selectExpr("CAST(value AS STRING)")
      // 转换为Dataset类型
      .as[String]
      // 过滤数据：通话状态为success
      .filter(record => null != record && record.trim.split(",").length > 0)

    // 自定义UDF函数，解析IP地址为省份和城市
    session.sparkContext.addFile(ApplicationConfig.IPS_DATA_REGION_PATH)

    // 【优化】使用 lazy val 在 Executor 端延迟初始化 DbSearcher，避免每条记录都创建新实例
    // 注意：SparkFiles.get 必须在 Executor 端执行，不能在 Driver 端调用
    @transient lazy val dbSearcher: DbSearcher = new DbSearcher(new DbConfig(), SparkFiles.get("ip2region.db"))

    val ip_to_location: UserDefinedFunction = udf((ip: String) => {
      // ========== 方案一核心修改：在 UDF 内部处理 null 和异常情况 ==========
      if (ip == null || ip.trim.isEmpty) {
        // IP 为空时返回默认值
        ("Unknown", "Unknown")
      } else {
        try {
          // 依据IP地址解析
          val dataBlock: DataBlock = dbSearcher.btreeSearch(ip.trim)
          if (dataBlock == null) {
            ("Unknown", "Unknown")
          } else {
            // 中国|0|海南省|海口市|教育网
            val region: String = dataBlock.getRegion
            println("======>" + region)
            // 分割字符串，获取省份和城市
            val parts = region.split("\\|")
            if (parts.length >= 4) {
              (parts(2), parts(3))
            } else {
              ("Unknown", "Unknown")
            }
          }
        } catch {
          case e: Exception =>
            logWarning(s"Failed to parse IP: $ip, error: ${e.getMessage}")
            ("Unknown", "Unknown")
        }
      }
      // ====================================================================
    })

    // 2. 其他订单字段，按照订单状态过滤和转换IP地址
    val resultStreamDF: DataFrame = recordStreamDS
      // 提取订单字段
      .select(
        get_json_object($"value", "$.orderId").as("orderId"),
        get_json_object($"value", "$.userId").as("userId"),
        get_json_object($"value", "$.orderTime").as("orderTime"),
        get_json_object($"value", "$.ip").as("ip"),
        get_json_object($"value", "$.orderMoney").as("orderMoney"),
        get_json_object($"value", "$.orderStatus").cast(IntegerType).as("orderStatus")
      )
      // 订单状态为0(打开)
      .filter($"orderStatus" === 0)
      // 解析IP地址为省份和城市
      .withColumn("location", ip_to_location($"ip"))
      // 获取省份和城市列
      .withColumn("province", $"location._1")
      .withColumn("city", $"location._2")
      .select(
        $"orderId".as("key"),
        to_json(
          struct(
            $"orderId", $"userId", $"orderTime", $"ip",
            $"orderMoney", $"orderStatus", $"province", $"city"
          )
        ).as("value")
      )

    // 3. 返回
    resultStreamDF
  }

  def main(args: Array[String]): Unit = {
    // 1. 获取SparkSession实例对象
    val spark: SparkSession = SparkUtils.createSparkSession(this.getClass)
    import spark.implicits._

    // 2. 从KAFKA读取消费数据
    val kafkaStreamDF: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("subscribe", ApplicationConfig.KAFKA_SOURCE_TOPICS)
      // 设置每批次消费数据最大值
      .option("maxOffsetsPerTrigger", ApplicationConfig.KAFKA_MAX_OFFSETS)
      .load()

    // 3. 数据ETL操作
    val etlStreamDF: DataFrame = streamingProcess(kafkaStreamDF)

    // 4. 针对流式应用来说，输出的是流
    val query: StreamingQuery = etlStreamDF.writeStream
      // 对流式应用输出来说，设置输出模式
      .outputMode(OutputMode.Append())
      .format("kafka")
      .option("kafka.bootstrap.servers", ApplicationConfig.KAFKA_BOOTSTRAP_SERVERS)
      .option("topic", ApplicationConfig.KAFKA_ETL_TOPIC)
      // 设置检查点目录
      .option("checkpointLocation", ApplicationConfig.STREAMING_ETL_CKPT)
      // 流式应用，需要启动start
      .start()

    // 5. 流式查询等待流式应用终止
    query.awaitTermination()
    query.stop()
  }
}
