package com.myexercise3.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @Author: Zhangbao
 * @Date: 0:46 2020/8/17
 * @Description: 与Redis交互的工具类，创建Redis连接池
 *               获取Redis客户端
 */
object RedisUtil3 {
  var jedisPool: JedisPool = _
  def getJedisClinet : Jedis = {
    if (jedisPool == null) {
      println("创建一个Redis连接池")
      val properties: Properties = PropertiesUtil3.load("config.properties")
      val host: String = properties.getProperty("redis.host")
      val port: String = properties.getProperty("redis.port")

      val jedisPoolConfig: JedisPoolConfig = new JedisPoolConfig // 创建连接池配置对象
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲连接数
      jedisPoolConfig.setMinIdle(10) //最小空闲连接数
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌是否等待
      jedisPoolConfig.setMaxWaitMillis(500) //忙碌最大等待时间
      jedisPoolConfig.setTestOnBorrow(true) //每次借到连接进行测试
      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt) //创建一个Redis客户端连接池
    }
    jedisPool.getResource
  }
}
