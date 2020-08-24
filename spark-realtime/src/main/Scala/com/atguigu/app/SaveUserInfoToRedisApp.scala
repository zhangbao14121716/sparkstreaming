package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis


/**
 * @Author: Zhangbao
 * @Date: 14:38 2020/8/21
 * @Description:
 *
 */
object SaveUserInfoToRedisApp {

  def main(args: Array[String]): Unit = {
    //1.创建配置对象，和sparkStreaming对象
    val sparkConf: SparkConf = new SparkConf().setAppName("SaveUserInfoToRedisApp").setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))
    //2.获取kafka流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_USER, ssc)
    //3.写入Redis操作
    kafkaDStream.foreachRDD(rdd => {
      //减少连接数
      rdd.foreachPartition(iter =>{
        //创建连接
        val jedisClient: Jedis = RedisUtil.getJedisClient
        //获取数据
        iter.foreach(record => {
          val userInfoOrgin: String = record.value()
          val userInfo: UserInfo = JSON.parseObject(userInfoOrgin, classOf[UserInfo])
          val redisKey: String = s"userInfo:${userInfo.id}"
          //将userInfo写入数据
          jedisClient.set(redisKey,userInfoOrgin)
        })
        //释放连接
        jedisClient.close()
      })
    })

    //启动任务
    ssc.start()
    ssc.awaitTermination()


  }
}
