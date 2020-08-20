package com.atguigu.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
/**
 * @Author: Zhangbao
 * @Date: 14:20 2020/8/15
 * @Description:
 *
 * 作用：与Redis交互的工具类，用来获取Redis客户端
 *
 */
object RedisUtil {

  var jedisPool: JedisPool = _

  def getJedisClient: Jedis = {
    if (jedisPool == null) {
      println("开辟一个连接池")
      val config: Properties = PropertiesUtil.load("config.properties")
      val host: String = config.getProperty("redis.host")
      val port: String = config.getProperty("redis.port")

      val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
//    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
//    println("获得一个连接")
    jedisPool.getResource
  }
}