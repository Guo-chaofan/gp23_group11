package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Redis连接
  */
object JedisConnectionPool {

  val config = new JedisPoolConfig()

  config.setMaxTotal(20)

  config.setMaxIdle(10)

<<<<<<< HEAD
  private val pool = new JedisPool(config,"node01",6379,10000,"123456")
=======
  private val pool = new JedisPool(config,"node4",6379,10000,"123")
>>>>>>> b0a790f978543b5a1c5d8c45a93ba7f96e2c668d

  def getConnection():Jedis={
    pool.getResource
  }

}
