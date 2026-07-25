package net.togogo.spark.utils

import net.togogo.spark.config.ApplicationConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
    工具类：构建SparkSession和StreamingContext实例对象
*/
object SparkUtils {
  /**
   * 获取SparkSession实例对象，传递Class对象
   *
   * @param clazz Spark Application字节码Class对象
   * @return SparkSession对象实例
   */
  def createSparkSession(clazz: Class[_], isRedis: Boolean = false): SparkSession = {
    // 1. 构建SparkConf对象
    val sparkConf: SparkConf = new SparkConf()
      .setAppName(clazz.getSimpleName.stripSuffix("$"))
      .set("spark.debug.maxToStringFields", "2000")
      .set("spark.sql.debug.maxToStringFields", "2000")
    // 2. 判断应用是否本地模式运行，如果是设置值
    if (ApplicationConfig.APP_LOCAL_MODE) {
      sparkConf
        .setMaster(ApplicationConfig.APP_SPARK_MASTER)
        // 设置Shuffle时分区数目
        .set("spark.sql.shuffle.partitions", "3")
    }
    // 3. 获取SparkSession实例对象
    val session: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
    // 4. 返回实例
    session
  }

  /**
   * 获取StreamingContext流式上下文实例对象
   *
   * @param clazz         Spark Application字节码Class对象
   * @param batchInterval 每批次时间间隔
   */
  def createStreamingContext(clazz: Class[_], batchInterval: Int): StreamingContext = {
    // 构建对象实例
    val context: StreamingContext = StreamingContext.getActiveOrCreate(
      () => {
        // 1. 构建SparkConf对象
        val sparkConf: SparkConf = new SparkConf()
          .setAppName(clazz.getSimpleName.stripSuffix("$"))
          .set("spark.debug.maxToStringFields", "2000")
          .set("spark.sql.debug.maxToStringFields", "2000")
          .set("spark.streaming.stopGracefullyOnShutdown", "true")
        // 2. 判断应用是否本地模式运行，如果是设置值
        if (ApplicationConfig.APP_LOCAL_MODE) {
          sparkConf.setMaster(ApplicationConfig.APP_SPARK_MASTER)
            // 设置每批次消费数据最大数据量，生成环境使用命令行设置
            .set("spark.streaming.kafka.maxRatePerPartition", "10000")
        }
        // 3. 创建StreamingContext对象
        new StreamingContext(sparkConf, Seconds(batchInterval))
      }
    )
    context // 返回对象
  }
}