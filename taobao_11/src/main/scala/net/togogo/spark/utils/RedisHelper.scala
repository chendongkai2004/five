package net.togogo.spark.utils

import org.apache.spark.sql.{DataFrame, Row}
import net.togogo.spark.config.ApplicationConfig

object RedisHelper {
  def saveToRedis(batchDF: DataFrame): Unit = {
    batchDF.rdd.foreachPartition { iter =>
      val jedis = JedisUtils.getJedisPoolInstance(ApplicationConfig.REDIS_HOST, ApplicationConfig.REDIS_PORT.toInt).getResource
      jedis.select(ApplicationConfig.REDIS_DB.toInt)
      iter.foreach { row =>
        val key = row.getAs[String]("key")
        val field = row.getAs[String]("field")
        val value = row.getAs[String]("value")
        jedis.hset(key, field, value)
      }
      JedisUtils.release(jedis)
    }
  }
}
