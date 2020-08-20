package com.myexercise3.handler

import java.time.LocalDate
import java.util

import com.myexercise3.bean.StartLogUp3
import com.myexercise3.utils.RedisUtil3
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis
import org.apache.phoenix.spark._

/**
 * @Author: Zhangbao
 * @Date: 20:51 2020/8/17
 * @Description:
 *
 */
object DauHandler3 {



  //将数据进行跨批次去重
  def filterByRedis(startLogUpDStream: DStream[StartLogUp3], sc: SparkContext): DStream[StartLogUp3] = {
    val filterByBroadcastDStream: DStream[StartLogUp3] = startLogUpDStream.transform(rdd => {
      //获取Redis连接
      val jedisClinet: Jedis = RedisUtil3.getJedisClinet
      //将上一状态redis中所有数据取出
      val todayDate: String = LocalDate.now().toString
      val radisSet: util.Set[String] = jedisClinet.smembers(s"dau:${todayDate}")
      val midBroadcast: Broadcast[util.Set[String]] = sc.broadcast(radisSet)
      //归还连接
      jedisClinet.close()
      //用广播变量对数据进行过滤
      rdd.filter(startLog => {
        !midBroadcast.value.contains(startLog.mid)
      })
    })
    filterByBroadcastDStream
  }

  //将数据进行同批次去重
  def filterByGroup(starLogUpDStream: DStream[StartLogUp3], sc: SparkContext) : DStream[StartLogUp3] = {
    val dateAndhourDStream: DStream[(String, StartLogUp3)] = starLogUpDStream.map(startLog => {
      (s"dau:${startLog.logDate}-${startLog.logHour}", startLog)
    })
    val listDStream: DStream[(String, List[StartLogUp3])] = dateAndhourDStream.groupByKey().mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })
    listDStream.flatMap { case (_, list) => list }
  }

  //将数据写入Redis中
  def saveToRedis(startLogUpDStream: DStream[StartLogUp3]) : Unit = {
    //将DStream转化成RDD进行操作
    startLogUpDStream.foreachRDD(rdd => {
      //用foreachPartition代替foreach，减少数据库的连接数
      rdd.foreachPartition(iter => {
        //获取Redis连接
        val jedisClinet: Jedis = RedisUtil3.getJedisClinet
        //每个分区的进行写数据操作
        iter.foreach(startLog => {
          val radisKey: String = s"dau:${startLog.logDate}"
          jedisClinet.sadd(radisKey,startLog.mid)
        })
        //归还数据库连接
        jedisClinet.close()
      })
    })
  }

  //将数据写入HBase
  def saveToHBase(startLogUpStream : DStream[StartLogUp3], tableName: String) = {
    startLogUpStream.foreachRDD(rdd => {
      val cols: Array[String] = classOf[StartLogUp3].getDeclaredFields.map(_.getName.toUpperCase())
      rdd.saveToPhoenix(
        tableName,
        cols,
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })
  }

}
