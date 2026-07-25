package net.togogo.spark.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisUtils {
  def getJedisPoolInstance(host: String, port: Int): JedisPool = {
    val poolConfig = new JedisPoolConfig
    poolConfig.setMaxTotal(10)
    poolConfig.setMaxIdle(5)
    poolConfig.setMinIdle(2)
    new JedisPool(poolConfig, host, port)
  }

  def release(jedis: Jedis): Unit = jedis.close()
}
